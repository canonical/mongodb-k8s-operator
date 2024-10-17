#!/usr/bin/env python3
"""Handles kubernetes services and webhook creation."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import base64
from logging import getLogger

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.admissionregistration_v1 import (
    MutatingWebhook,
    RuleWithOperations,
    ServiceReference,
    WebhookClientConfig,
)
from lightkube.models.core_v1 import ServicePort, ServiceSpec
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources.admissionregistration_v1 import MutatingWebhookConfiguration
from lightkube.resources.core_v1 import Pod, Service
from ops.model import Unit

from config import Config

logger = getLogger()


def get_pod(client: Client, pod_name: str) -> Pod:
    """Gets a pod definition from k8s."""
    try:
        pod = client.get(res=Pod, name=pod_name)
    except ApiError:
        raise
    return pod


def generate_service(client: Client, unit: Unit, model_name: str):
    """Generates the k8s service for the mutating webhook."""
    pod_name = unit.name.replace("/", "-")
    pod = get_pod(client, pod_name)
    if not pod.metadata:
        raise Exception(f"Could not find metadata for {pod}")

    try:
        service = Service(
            metadata=ObjectMeta(
                name=Config.WebhookManager.SERVICE_NAME,
                namespace=model_name,
                ownerReferences=[
                    OwnerReference(
                        apiVersion=pod.apiVersion,
                        kind=pod.kind,
                        name=pod_name,
                        uid=pod.metadata.uid,
                        blockOwnerDeletion=False,
                    )
                ],
            ),
            spec=ServiceSpec(
                type="ClusterIP",
                selector={"statefulset.kubernetes.io/pod-name": pod_name},
                ports=[
                    ServicePort(
                        protocol="TCP",
                        port=Config.WebhookManager.PORT,
                        targetPort=Config.WebhookManager.PORT,
                        name=f"{Config.WebhookManager.SERVICE_NAME}-port",
                    ),
                ],
            ),
        )
        client.create(service)
    except ApiError:
        logger.info("Not creating a service, already present")


def generate_mutating_webhook(client: Client, unit: Unit, model_name: str, cert: str):
    """Generates the mutating webhook for this application."""
    pod_name = unit.name.replace("/", "-")
    pod = get_pod(client, pod_name)
    app_name = unit.name.split("/")[0]
    try:
        webhooks = client.get(
            MutatingWebhookConfiguration,
            namespace=model_name,
            name=Config.WebhookManager.SERVICE_NAME,
        )
        if webhooks:
            return
    except ApiError:
        logger.debug("Mutating Webhook doesn't yet exist.")

    ca_bundle = base64.b64encode(cert.encode()).decode()

    logger.debug("Registering our Mutating Wehook.")
    webhook_config = MutatingWebhookConfiguration(
        metadata=ObjectMeta(
            name=Config.WebhookManager.SERVICE_NAME,
            namespace=model_name,
            ownerReferences=pod.metadata.ownerReferences,
        ),
        apiVersion="admissionregistration.k8s.io/v1",
        webhooks=[
            MutatingWebhook(
                name=f"{app_name}.juju.is",
                clientConfig=WebhookClientConfig(
                    service=ServiceReference(
                        namespace=model_name,
                        name=Config.WebhookManager.SERVICE_NAME,
                        port=8000,
                        path="/mutate",
                    ),
                    caBundle=ca_bundle,
                ),
                rules=[
                    RuleWithOperations(
                        operations=["CREATE", "UPDATE"],
                        apiGroups=["apps"],
                        apiVersions=["v1"],
                        resources=["statefulsets"],
                    )
                ],
                admissionReviewVersions=["v1"],
                sideEffects="None",
                timeoutSeconds=5,
            )
        ],
    )
    client.create(webhook_config)
