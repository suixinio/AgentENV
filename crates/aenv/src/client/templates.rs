use super::{handle_status, Client};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct CreateTemplateV3 {
    pub name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "cpuCount")]
    pub cpu_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "memoryMB")]
    pub memory_mb: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct StartTemplateBuildV2 {
    #[serde(skip_serializing_if = "Option::is_none", rename = "fromImage")]
    pub from_image: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<TemplateStep>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "startCmd")]
    pub start_cmd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "readyCmd")]
    pub ready_cmd: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TemplateStep {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct TemplateV3Response {
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(rename = "buildID")]
    pub build_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Template {
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(rename = "buildID")]
    pub build_id: String,
    #[serde(default, rename = "buildStatus")]
    pub build_status: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default, rename = "cpuCount")]
    pub cpu_count: Option<u32>,
    #[serde(default, rename = "memoryMB")]
    pub memory_mib: Option<u32>,
    #[serde(default, rename = "diskSizeMB")]
    pub disk_size_mib: Option<u32>,
    #[serde(default)]
    pub public: Option<bool>,
    #[serde(default, rename = "spawnCount")]
    pub spawn_count: Option<u64>,
    #[serde(default, rename = "createdAt")]
    pub created_at: Option<String>,
    #[serde(default, rename = "updatedAt")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TemplateAliasResponse {
    #[serde(rename = "templateID")]
    pub template_id: String,
}

#[derive(Debug, Deserialize)]
pub struct TemplateBuildInfo {
    #[serde(rename = "templateID")]
    pub template_id: String,
    #[serde(rename = "buildID")]
    pub build_id: String,
    pub status: String,
    #[serde(default)]
    pub reason: Option<BuildStatusReason>,
}

#[derive(Debug, Deserialize)]
pub struct BuildStatusReason {
    pub message: String,
    #[serde(default)]
    pub step: Option<String>,
}

impl Client {
    /// Returns all templates by following pagination cursors.
    pub fn list_templates(&self) -> Result<Vec<Template>> {
        let mut templates = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let mut request = self.get("/v2/templates").query("limit", "100");
            if let Some(token) = next_token.as_deref() {
                request = request.query("nextToken", token);
            }

            let resp = handle_status(request.call())?;
            next_token = resp
                .header("x-next-token")
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(str::to_string);
            let mut page: Vec<Template> = resp.into_json()?;
            templates.append(&mut page);

            if next_token.is_none() {
                break;
            }
        }

        Ok(templates)
    }

    pub fn resolve_alias(&self, alias: &str) -> Result<String> {
        let resp = handle_status(self.get(&format!("/templates/aliases/{}", alias)).call())?;
        let body: TemplateAliasResponse = resp.into_json()?;
        Ok(body.template_id)
    }

    pub fn delete_template(&self, id: &str) -> Result<()> {
        handle_status(self.delete(&format!("/templates/{}", id)).call())?;
        Ok(())
    }

    pub fn create_template_v3(&self, req: &CreateTemplateV3) -> Result<TemplateV3Response> {
        let resp = handle_status(self.post("/v3/templates").send_json(req))?;
        Ok(resp.into_json()?)
    }

    pub fn start_template_build_v2(
        &self,
        template_id: &str,
        build_id: &str,
        req: &StartTemplateBuildV2,
    ) -> Result<()> {
        handle_status(
            self.post(&format!("/v2/templates/{template_id}/builds/{build_id}"))
                .send_json(req),
        )?;
        Ok(())
    }

    pub fn template_build_status(
        &self,
        template_id: &str,
        build_id: &str,
    ) -> Result<TemplateBuildInfo> {
        let resp = handle_status(
            self.get(&format!(
                "/templates/{template_id}/builds/{build_id}/status"
            ))
            .call(),
        )?;
        Ok(resp.into_json()?)
    }
}
