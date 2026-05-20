use iroh::{Endpoint, RelayMode, SecretKey, RelayConfig, RelayUrl};
use iroh::endpoint::presets;
use std::sync::Arc;
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TailscaleDerpMap {
    #[serde(rename = "Regions")]
    region_map: HashMap<u32, Region>,
}

#[derive(Debug, Deserialize)]
struct Region {
    #[serde(rename = "Nodes")]
    nodes: Vec<Node>,
}

#[derive(Debug, Deserialize)]
struct Node {
    #[serde(rename = "HostName")]
    host_name: String,
}

pub struct IrohNetworking {
    endpoint: Endpoint,
}

impl IrohNetworking {
    pub async fn new(secret_key: SecretKey, custom_relays: Vec<String>) -> Result<Self> {
        let relay_map = RelayMode::Default.relay_map();

        // Dynamically fetch Tailscale DERP servers
        if let Ok(response) = reqwest::get("https://controlplane.tailscale.com/derpmap/default").await {
            if let Ok(derp_map) = response.json::<TailscaleDerpMap>().await {
                for region in derp_map.region_map.values() {
                    for node in &region.nodes {
                        let url_str = format!("https://{}", node.host_name);
                        if let Ok(url) = url_str.parse::<RelayUrl>() {
                            relay_map.insert(url.clone(), Arc::new(RelayConfig::from(url)));
                        }
                    }
                }
            }
        }

        for url_str in custom_relays {
            if let Ok(url) = url_str.parse::<RelayUrl>() {
                relay_map.insert(url.clone(), Arc::new(RelayConfig::from(url)));
            }
        }

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .relay_mode(RelayMode::Custom(relay_map))
            .alpns(vec![b"stun-moq/0.1".to_vec()])
            .bind()
            .await?;

        Ok(Self { endpoint })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }
}
