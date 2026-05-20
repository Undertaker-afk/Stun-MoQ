use iroh::{Endpoint, RelayMode, SecretKey, RelayConfig, RelayUrl};
use iroh::endpoint::{presets, QuicTransportConfig, VarInt};
use std::sync::Arc;
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{info, debug, warn};

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
    #[serde(rename = "DERPPort")]
    derp_port: Option<u16>,
}

pub struct IrohNetworking {
    endpoint: Endpoint,
}

impl IrohNetworking {
    pub async fn new(secret_key: SecretKey, custom_relays: Vec<String>) -> Result<Self> {
        info!("Initializing Iroh networking...");
        let relay_map = RelayMode::Default.relay_map();

        // Dynamically fetch Tailscale DERP servers from the correct URL
        debug!("Fetching Tailscale DERP map from login.tailscale.com...");
        if let Ok(response) = reqwest::get("https://login.tailscale.com/derpmap/default").await {
            if let Ok(derp_map) = response.json::<TailscaleDerpMap>().await {
                debug!("Fetched {} DERP regions", derp_map.region_map.len());
                for region in derp_map.region_map.values() {
                    for node in &region.nodes {
                        let port = node.derp_port.unwrap_or(443);
                        let url_str = if port == 443 {
                            format!("https://{}", node.host_name)
                        } else {
                            format!("https://{}:{}", node.host_name, port)
                        };

                        if let Ok(url) = url_str.parse::<RelayUrl>() {
                            relay_map.insert(url.clone(), Arc::new(RelayConfig::from(url)));
                        }
                    }
                }
            } else {
                warn!("Failed to parse Tailscale DERP map JSON");
            }
        } else {
            warn!("Failed to fetch Tailscale DERP map from login.tailscale.com");
        }

        for url_str in custom_relays {
            if let Ok(url) = url_str.parse::<RelayUrl>() {
                relay_map.insert(url.clone(), Arc::new(RelayConfig::from(url)));
            }
        }

        // Optimize QUIC parameters for high-bandwidth/low-latency
        let transport_config = QuicTransportConfig::builder()
            .max_concurrent_uni_streams(VarInt::from_u32(1024))
            .stream_receive_window(VarInt::from_u32(1024 * 1024 * 16)) // 16MB
            .receive_window(VarInt::from_u32(1024 * 1024 * 64)) // 64MB
            .build();

        info!("Starting Iroh endpoint with {} relays...", relay_map.len());
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .relay_mode(RelayMode::Custom(relay_map))
            .transport_config(transport_config)
            .alpns(vec![b"stun-moq/0.1".to_vec()])
            .bind()
            .await?;

        // Wait for the endpoint to find its home relay (latency-based selection)
        debug!("Waiting for endpoint to find optimal home relay...");
        endpoint.online().await;

        info!("Iroh endpoint online. Node ID: {}", endpoint.id());
        Ok(Self { endpoint })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn addr(&self) -> iroh::EndpointAddr {
        self.endpoint.addr()
    }
}
