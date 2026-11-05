/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use typeql::{
    annotation::Annotation,
    common::token,
    query::{Query, QueryStructure, SchemaQuery},
    schema::definable::{
        Definable, Type as TypeDeclaration,
        type_::{Capability, CapabilityBase},
    },
    type_::{NamedType, TypeRef, TypeRefAny},
};

#[derive(Debug, Default)]
pub(crate) struct Schema {
    pub(crate) attributes: Vec<AttributeType>,
    pub(crate) objects: Vec<ObjectType>,
}

impl Schema {
    pub(crate) fn parse(schema_text: &str) -> Result<Self, String> {
        let declarations = type_declarations(schema_text)?;
        let mut schema = Schema::default();
        schema.declare_types(&declarations);
        schema.apply_capabilities(declarations)?;
        schema.refuse_unknown_supertypes()?;
        schema.refuse_reserved_labels()?;
        Ok(schema)
    }

    pub(crate) fn attribute(&self, label: &str) -> Option<&AttributeType> {
        self.attributes.iter().find(|attribute| attribute.label == label)
    }

    pub(crate) fn object(&self, label: &str, kind: ObjectKind) -> Option<&ObjectType> {
        self.objects.iter().find(|object| object.label == label && object.kind == kind)
    }

    pub(crate) fn objects(&self, kind: ObjectKind) -> impl Iterator<Item = &ObjectType> + Clone {
        self.objects.iter().filter(move |object| object.kind == kind)
    }

    pub(crate) fn value_type(&self, attribute: &AttributeType) -> Option<ValueType> {
        let supertype = |attribute: &AttributeType| self.attribute(attribute.supertype.as_deref()?);
        std::iter::successors(Some(attribute), |attribute| supertype(attribute))
            .take(self.attributes.len())
            .find_map(|attribute| attribute.value_type)
    }

    pub(crate) fn owns(&self, object: &ObjectType, attribute: &str) -> bool {
        self.object_and_supertypes(object).any(|object| object.owns.iter().any(|owned| owned == attribute))
    }

    pub(crate) fn plays<'a>(&'a self, object: &'a ObjectType) -> impl Iterator<Item = &'a Plays> {
        self.object_and_supertypes(object).flat_map(|object| &object.plays)
    }

    pub(crate) fn roles_of<'a>(&'a self, relation: &'a ObjectType) -> Vec<&'a RoleType> {
        let (mut roles, mut specialised) = (Vec::new(), HashSet::new());
        for relation in self.object_and_supertypes(relation) {
            roles.extend(relation.roles.iter().filter(|role| !specialised.contains(&role.label)));
            specialised.extend(relation.roles.iter().filter_map(|role| role.specialises.as_ref()));
        }
        roles
    }

    fn object_and_supertypes<'a>(&'a self, object: &'a ObjectType) -> impl Iterator<Item = &'a ObjectType> {
        let supertype = |object: &ObjectType| self.object(object.supertype.as_deref()?, object.kind);
        std::iter::successors(Some(object), move |object| supertype(object)).take(self.objects.len())
    }

    fn declare_types(&mut self, declarations: &[TypeDeclaration]) {
        for declaration in declarations {
            let label = declaration.label.ident.to_string();
            match declaration.kind {
                None | Some(token::Kind::Role) => {}
                Some(token::Kind::Attribute) => self.declare_attribute(label),
                Some(token::Kind::Entity) => self.declare_object(label, ObjectKind::Entity),
                Some(token::Kind::Relation) => self.declare_object(label, ObjectKind::Relation),
            }
        }
    }

    fn declare_attribute(&mut self, label: String) {
        if self.attribute(&label).is_none() {
            self.attributes.push(AttributeType::new(label));
        }
    }

    fn declare_object(&mut self, label: String, kind: ObjectKind) {
        if self.object(&label, kind).is_none() {
            self.objects.push(ObjectType::new(label, kind));
        }
    }

    fn apply_capabilities(&mut self, declarations: Vec<TypeDeclaration>) -> Result<(), String> {
        for declaration in declarations {
            let label = declaration.label.ident.to_string();
            let is_abstract =
                declaration.annotations.iter().any(|annotation| matches!(annotation, Annotation::Abstract(_)));
            if let Some(attribute) = self.attributes.iter_mut().find(|attribute| attribute.label == label) {
                attribute.is_abstract |= is_abstract;
                for capability in declaration.capabilities {
                    attribute.apply(capability)?;
                }
                continue;
            }
            let Some(object) = self.objects.iter_mut().find(|object| object.label == label) else {
                return Err(format!(
                    "type '{label}' has no kinded declaration (entity/relation/attribute) in this schema"
                ));
            };
            object.is_abstract |= is_abstract;
            for capability in declaration.capabilities {
                object.apply(capability)?;
            }
        }
        Ok(())
    }

    fn refuse_unknown_supertypes(&self) -> Result<(), String> {
        for attribute in &self.attributes {
            let unknown = attribute.supertype.as_deref().filter(|label| self.attribute(label).is_none());
            if let Some(supertype) = unknown {
                return Err(format!("'{}' extends unknown supertype '{supertype}'", attribute.label));
            }
        }
        for object in &self.objects {
            let unknown = object.supertype.as_deref().filter(|label| self.object(label, object.kind).is_none());
            if let Some(supertype) = unknown {
                return Err(format!("'{}' extends unknown supertype '{supertype}'", object.label));
            }
        }
        Ok(())
    }

    // The server's TypeQL refuses reserved keywords as labels, though its parser reads them.
    fn refuse_reserved_labels(&self) -> Result<(), String> {
        let mut labels = self.attributes.iter().map(|attribute| &attribute.label).chain(
            self.objects
                .iter()
                .flat_map(|object| std::iter::once(&object.label).chain(object.roles.iter().map(|role| &role.label))),
        );
        match labels.find(|label| typeql::is_reserved_keyword(label)) {
            Some(label) => Err(format!("'{label}' is a reserved TypeQL keyword, which TypeDB refuses as a label")),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct AttributeType {
    pub(crate) label: String,
    pub(crate) is_abstract: bool,
    pub(crate) supertype: Option<String>,
    pub(crate) value_type: Option<ValueType>,
}

impl AttributeType {
    fn new(label: String) -> Self {
        AttributeType { label, is_abstract: false, supertype: None, value_type: None }
    }

    fn apply(&mut self, capability: Capability) -> Result<(), String> {
        match capability.base {
            CapabilityBase::Sub(sub) => self.supertype = Some(sub.supertype_label.ident.to_string()),
            CapabilityBase::ValueType(value_type) => {
                self.value_type = Some(parse_value_type(&value_type.value_type, &self.label)?)
            }
            other => {
                return Err(format!("attribute '{}' cannot declare '{other}' (only sub and value)", self.label));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ObjectType {
    pub(crate) label: String,
    pub(crate) kind: ObjectKind,
    pub(crate) is_abstract: bool,
    pub(crate) supertype: Option<String>,
    pub(crate) owns: Vec<String>,
    pub(crate) plays: Vec<Plays>,
    pub(crate) roles: Vec<RoleType>,
}

impl ObjectType {
    fn new(label: String, kind: ObjectKind) -> Self {
        ObjectType {
            label,
            kind,
            is_abstract: false,
            supertype: None,
            owns: Vec::new(),
            plays: Vec::new(),
            roles: Vec::new(),
        }
    }

    fn apply(&mut self, capability: Capability) -> Result<(), String> {
        match capability.base {
            CapabilityBase::Sub(sub) => self.supertype = Some(sub.supertype_label.ident.to_string()),
            CapabilityBase::Owns(owns) => {
                let attribute = type_ref_label(&owns.owned).ok_or_else(|| {
                    format!(
                        "'{} owns {}': the loader writes unordered ownerships, so it cannot load ordered ones",
                        self.label, owns.owned
                    )
                })?;
                self.owns.push(attribute);
            }
            CapabilityBase::Plays(plays) => self
                .plays
                .push(Plays { relation: plays.role.scope.ident.to_string(), role: plays.role.name.ident.to_string() }),
            CapabilityBase::Relates(relates) if self.kind == ObjectKind::Relation => {
                let role = type_ref_label(&relates.related).ok_or_else(|| {
                    format!(
                        "'{} relates {}': the loader writes unordered role players, so it cannot load ordered ones",
                        self.label, relates.related
                    )
                })?;
                let specialises = relates.specialised.as_ref().and_then(type_ref_label);
                let is_abstract =
                    capability.annotations.iter().any(|annotation| matches!(annotation, Annotation::Abstract(_)));
                match self.roles.iter_mut().find(|declared| declared.label == role) {
                    Some(declared) => declared.is_abstract |= is_abstract,
                    None => {
                        let relation = self.label.clone();
                        self.roles.push(RoleType { label: role, relation, specialises, is_abstract });
                    }
                }
            }
            other => {
                return Err(format!("{} '{}' cannot declare '{other}'", self.kind.name(), self.label));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectKind {
    Entity,
    Relation,
}

impl ObjectKind {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            ObjectKind::Entity => "entity",
            ObjectKind::Relation => "relation",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Plays {
    pub(crate) relation: String,
    pub(crate) role: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RoleType {
    pub(crate) label: String,
    // The relation that declares the role; subtypes inherit it under this scope.
    pub(crate) relation: String,
    pub(crate) specialises: Option<String>,
    pub(crate) is_abstract: bool,
}

impl RoleType {
    pub(crate) fn scoped_label(&self) -> String {
        format!("{}:{}", self.relation, self.label)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueType {
    Boolean,
    Integer,
    Double,
    Decimal,
    String,
    Date,
    Datetime,
    DatetimeTz,
    Duration,
}

impl ValueType {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Double => "double",
            Self::Decimal => "decimal",
            Self::String => "string",
            Self::Date => "date",
            Self::Datetime => "datetime",
            Self::DatetimeTz => "datetime-tz",
            Self::Duration => "duration",
        }
    }
}

fn type_declarations(schema_text: &str) -> Result<Vec<TypeDeclaration>, String> {
    let queries =
        typeql::parse_queries(schema_text).map_err(|err| format!("failed to parse the TypeDB schema: {err}"))?;
    let define = match <[Query; 1]>::try_from(queries) {
        Ok([Query { structure: QueryStructure::Schema(SchemaQuery::Define(define)), .. }]) => define,
        _ => return Err("the schema file must contain a single `define` query".to_owned()),
    };
    let declarations = define.definables.into_iter().filter_map(|definable| match definable {
        Definable::TypeDeclaration(declaration) => Some(declaration),
        Definable::Function(_) | Definable::Struct(_) | Definable::TypeRename(_) | Definable::RoleTypeRename(_) => None,
    });
    Ok(declarations.collect())
}

fn type_ref_label(type_ref: &TypeRefAny) -> Option<String> {
    match type_ref {
        TypeRefAny::Type(TypeRef::Label(label)) => Some(label.ident.to_string()),
        TypeRefAny::Type(TypeRef::Scoped(scoped)) => Some(scoped.name.ident.to_string()),
        _ => None,
    }
}

fn parse_value_type(named: &NamedType, attribute: &str) -> Result<ValueType, String> {
    match named {
        NamedType::BuiltinValueType(builtin) => Ok(match builtin.token {
            token::ValueType::Boolean => ValueType::Boolean,
            token::ValueType::Integer => ValueType::Integer,
            token::ValueType::Double => ValueType::Double,
            token::ValueType::Decimal => ValueType::Decimal,
            token::ValueType::String => ValueType::String,
            token::ValueType::Date => ValueType::Date,
            token::ValueType::DateTime => ValueType::Datetime,
            token::ValueType::DateTimeTZ => ValueType::DatetimeTz,
            token::ValueType::Duration => ValueType::Duration,
        }),
        NamedType::Label(label) => {
            let label = &label.ident;
            Err(format!("attribute '{attribute}' has struct value type '{label}', which the loader cannot load"))
        }
    }
}
