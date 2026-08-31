# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Application name"
  type        = string
  default     = "mongodb-k8s"
}

variable "base" {
  description = "The operating system on which to deploy. E.g. ubuntu@22.04."
  type        = string
  default     = "ubuntu@22.04"

  validation {
    condition     = var.base == "ubuntu@22.04"
    error_message = "The base variable only accepts ubuntu@22.04."
  }
}

variable "channel" {
  description = "Charm channel"
  type        = string
  default     = "6/stable"
}

variable "config" {
  description = "Map of charm configuration options"
  type        = map(string)
  default     = {}
}

variable "constraints" {
  description = "String listing constraints for this application"
  type        = string
  default     = "arch=amd64"
}

variable "expose" {
  description = "Expose the application for external access."
  type = list(object({
    cidrs     = optional(string)
    endpoints = optional(string)
    spaces    = optional(string)
  }))
  default = []
}

variable "model_uuid" {
  description = "Model UUID"
  type        = string
  nullable    = false
}

variable "revision" {
  description = "Charm revision"
  type        = number
  default     = null
}

variable "storage_directives" {
  description = "Map of storage directives (constraints) for the juju application"
  type        = map(string)
  default     = {}
}

variable "units" {
  description = "Charm units"
  type        = number
  default     = 3
}
