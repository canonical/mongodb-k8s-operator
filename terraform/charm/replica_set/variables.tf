# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Application name"
  type        = string
  default     = "mongodb-k8s"
}

variable "channel" {
  description = "Charm channel"
  type        = string
  default     = "6/stable"
}

variable "base" {
  description = "Charm base (old name: series)"
  type        = string
  default     = "ubuntu@22.04"
}

variable "config" {
  description = "Map of charm configuration options"
  type        = map(string)
  default     = {}
}

variable "model" {
  description = "Model name"
  type        = string
}

variable "revision" {
  description = "Charm revision"
  type        = number
  default     = null
}

variable "units" {
  description = "Charm units"
  type        = number
  default     = 3
}

variable "constraints" {
  description = "String listing constraints for this application"
  type        = string
  default     = "arch=amd64"
}

variable "machines" {
  description = "List of machines for placement"
  type        = set(string)
  default     = null
}

variable "storage" {
  description = "Map of storage used by the application"
  type        = map(string)
  default     = {}

  validation {
    condition     = length(var.storage) == 0 || lookup(var.storage, "count", 0) <= 1
    error_message = "Only one storage is supported"
  }
}

variable "endpoint_bindings" {
  description = "Map of endpoint bindings"
  type        = set(map(string))
  default     = []
}
