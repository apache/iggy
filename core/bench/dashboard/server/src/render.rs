// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use charming::Chart;
use charming::renderer::image_renderer::{ImageFormat, ImageRenderer};
use charming::theme::Theme;
use std::sync::mpsc;
use tracing::info;

struct RenderJob {
    chart: Chart,
    width: u32,
    height: u32,
    dark: bool,
    reply: tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>,
}

/// Dedicated render thread for PNG generation.
///
/// charming's SSR uses deno_core (V8) which has global state that is not
/// safe to use from multiple OS threads. All rendering is pinned to a single
/// thread; actix workers send jobs via channel and await results without blocking.
/// Disk caching ensures only the first request for a given chart incurs render cost.
pub struct PngRenderPool {
    sender: mpsc::Sender<RenderJob>,
}

impl PngRenderPool {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<RenderJob>();

        std::thread::Builder::new()
            .name("png-render".to_string())
            .spawn(move || {
                for job in rx {
                    let result = Self::render(&job.chart, job.width, job.height, job.dark);
                    let _ = job.reply.send(result);
                }
            })
            .expect("failed to spawn png render thread");

        info!("PNG render thread started");
        Self { sender: tx }
    }

    pub fn default_pool() -> Self {
        Self::new()
    }

    fn render(chart: &Chart, width: u32, height: u32, dark: bool) -> Result<Vec<u8>, String> {
        let mut renderer = ImageRenderer::new(width, height);
        if dark {
            renderer = renderer.theme(Theme::Dark);
        }
        renderer
            .render_format(ImageFormat::Png, chart)
            .map_err(|e| format!("PNG render failed: {e}"))
    }

    pub async fn render_png(
        &self,
        chart: Chart,
        width: u32,
        height: u32,
        dark: bool,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(RenderJob {
                chart,
                width,
                height,
                dark,
                reply: tx,
            })
            .map_err(|_| "render pool shut down".to_string())?;
        rx.await.map_err(|_| "render worker dropped".to_string())?
    }
}
