#!/usr/bin/env python3
"""Handles kubernetes services and webhook creation."""
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import base64
from logging import getLogger

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.models.admissionregistration_v1 import (
    MatchCondition,
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


def generate_service(client: Client, unit: Unit, model_name: str, service_name: str):
    """Generates the k8s service for the mutating webhook."""
    pod_name = unit.name.replace("/", "-")
    pod = get_pod(client, pod_name)
    app_name = unit.name.split("/")[0]
    if not pod.metadata:
        raise Exception(f"Could not find metadata for {pod}")

    try:
        service = Service(
            metadata=ObjectMeta(
                name=service_name,
                namespace=model_name,
                ownerReferences=[
                    OwnerReference(
                        apiVersion=pod.apiVersion,
                        kind=pod.kind,
                        name=pod_name,
                        uid=pod.metadata.uid,
                        blockOwnerDeletion=True,
                    )
                ],
            ),
            spec=ServiceSpec(
                type="ClusterIP",
                selector={"app.kubernetes.io/name": app_name},
                ports=[
                    ServicePort(
                        protocol="TCP",
                        port=Config.WebhookManager.PORT,
                        targetPort=Config.WebhookManager.PORT,
                        name=f"{service_name}-port",
                    ),
                ],
            ),
        )
        client.create(service)
    except ApiError:
        logger.info("Not creating a service, already present")


def generate_mutating_webhook(
    client: Client, unit: Unit, model_name: str, cert: str, service_name: str
):
    """Generates the mutating webhook for this application."""
    pod_name = unit.name.replace("/", "-")
    app_name = unit.name.split("/")[0]
    pod = get_pod(client, pod_name)
    try:
        webhooks = client.get(
            MutatingWebhookConfiguration,
            namespace=model_name,
            name=service_name,
        )
        if webhooks:
            return
    except ApiError:
        logger.debug("Mutating Webhook doesn't yet exist.")

    ca_bundle = base64.b64encode(cert.encode()).decode()

    logger.debug("Registering our Mutating Wehook.")
    webhook_config = MutatingWebhookConfiguration(
        metadata=ObjectMeta(
            name=service_name,
            namespace=model_name,
            ownerReferences=[
                OwnerReference(
                    apiVersion=pod.apiVersion,
                    kind=pod.kind,
                    name=pod_name,
                    uid=pod.metadata.uid,
                    blockOwnerDeletion=True,
                )
            ],
        ),
        apiVersion="admissionregistration.k8s.io/v1",
        webhooks=[
            MutatingWebhook(
                name=f"{service_name}.juju.is",
                clientConfig=WebhookClientConfig(
                    service=ServiceReference(
                        namespace=model_name,
                        name=service_name,
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
                matchConditions=[
                    MatchCondition(
                        name=f"match-sts-{app_name}",
                        expression=f'object.metadata.name == "{app_name}"',
                    )
                ],
            )
        ],
    )
    client.create(webhook_config)
