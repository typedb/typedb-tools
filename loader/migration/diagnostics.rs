/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use crate::PrepareError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationStage {
    Structure,
    Schema,
    Data,
}

impl fmt::Display for ValidationStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValidationStage::Structure => "structure",
            ValidationStage::Schema => "schema",
            ValidationStage::Data => "data",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub stage: ValidationStage,
    pub message: String,
}

impl Diagnostic {
    pub(crate) fn structure(message: impl Into<String>) -> Self {
        Self { stage: ValidationStage::Structure, message: message.into() }
    }

    pub(crate) fn schema(message: impl Into<String>) -> Self {
        Self { stage: ValidationStage::Schema, message: message.into() }
    }

    pub(crate) fn data(message: impl Into<String>) -> Self {
        Self { stage: ValidationStage::Data, message: message.into() }
    }
}

#[derive(Debug, Default)]
pub struct Diagnostics {
    pub errors: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl Diagnostics {
    pub(crate) fn error(&mut self, diagnostic: Diagnostic) {
        self.errors.push(diagnostic);
    }

    pub(crate) fn warning(&mut self, diagnostic: Diagnostic) {
        self.warnings.push(diagnostic);
    }

    pub(crate) fn ensure_valid(&mut self, mapping_summary: impl FnOnce() -> Vec<String>) -> Result<(), PrepareError> {
        if self.errors.is_empty() {
            return Ok(());
        }
        Err(PrepareError::Invalid {
            errors: std::mem::take(&mut self.errors),
            warnings: std::mem::take(&mut self.warnings),
            mapping_summary: mapping_summary(),
        })
    }
}
