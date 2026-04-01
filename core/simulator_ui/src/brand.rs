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

//! Brand assets for the Apache Iggy VSR simulator UI.
//!
//! Provides the embedded logo texture.

/// Holds pre-loaded brand textures so they can be painted every frame without
/// re-uploading to the GPU.
pub struct BrandAssets {
    pub logo: egui::TextureHandle,
}

impl BrandAssets {
    /// Load the Apache Iggy logo from the embedded PNG and register it with
    /// the egui renderer.  Call once during app initialisation.
    pub fn load(ctx: &egui::Context) -> Self {
        let logo_bytes =
            include_bytes!("../../../assets/logo/0.5x/iggy-apache-color-darkbg@0.5x.png");
        let img = image::load_from_memory(logo_bytes)
            .expect("embedded logo PNG must be valid")
            .to_rgba8();
        let size = [img.width() as usize, img.height() as usize];
        let color_image = egui::ColorImage::from_rgba_unmultiplied(size, &img);
        let logo = ctx.load_texture("iggy_logo", color_image, egui::TextureOptions::LINEAR);
        Self { logo }
    }
}
