variable "model_name" {
  description = "Model name"
  type        = string
}

variable "config_server_app_name" {
  description = "config-server app name"
  type        = string
  default     = "config-server"
}

variable "config_server_replicas" {
  description = "Node count"
  type        = number
  default     = 3
}

variable "shards" {
  description = "A list of shards containing their name and number of replicas"
  type = list(object({
    name     = string
    replicas = number
  }))
  default = [
    { name = "shard0", replicas = 3 },
    { name = "shard1", replicas = 3 }
  ]
}



