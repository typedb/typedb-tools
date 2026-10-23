/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use typeql::{
    annotation::Annotation,
    common::token,
    query::{QueryStructure, SchemaQuery},
    schema::definable::{
        Definable,
        type_::{Capability, CapabilityBase},
    },
    type_::{NamedType, TypeRef, TypeRefAny},
};

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

#[derive(Debug, Clone)]
pub(crate) struct AttributeType {
    pub(crate) label: String,
    pub(crate) abstract_: bool,
    pub(crate) supertype: Option<String>,
    pub(crate) value_type: Option<ValueType>,
}

#[derive(Debug, Clone)]
pub(crate) struct Plays {
    pub(crate) relation: String,
    pub(crate) role: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Relates {
    pub(crate) role: String,
    // Role labels are scoped to the declaring relation, also when inherited.
    pub(crate) declared_by: String,
    pub(crate) specialises: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectType {
    pub(crate) label: String,
    pub(crate) abstract_: bool,
    pub(crate) supertype: Option<String>,
    pub(crate) owns: Vec<String>,
    pub(crate) plays: Vec<Plays>,
    pub(crate) relates: Vec<Relates>,
}

#[derive(Debug, Default)]
pub(crate) struct Schema {
    pub(crate) attributes: Vec<AttributeType>,
    pub(crate) entities: Vec<ObjectType>,
    pub(crate) relations: Vec<ObjectType>,
}

impl Schema {
    pub(crate) fn parse(schema_text: &str) -> Result<Self, String> {
        let query =
            typeql::parse_query(schema_text).map_err(|err| format!("failed to parse the TypeDB schema: {err}"))?;
        let QueryStructure::Schema(SchemaQuery::Define(define)) = query.structure else {
            return Err("the schema file must contain a single `define` query".to_owned());
        };

        let mut declarations = Vec::new();
        for definable in define.definables {
            match definable {
                Definable::TypeDeclaration(declaration) => declarations.push(declaration),
                Definable::Function(_) => {}
                Definable::Struct(struct_) => {
                    return Err(format!(
                        "struct '{}' is not supported: TypeDB 3.x has no struct values",
                        struct_.ident
                    ));
                }
                Definable::TypeRename(_) | Definable::RoleTypeRename(_) => {
                    return Err("the schema file must define types, not rename them".to_owned());
                }
            }
        }

        // Types are registered first, so declarations and extensions can come in any order.
        let mut schema = Schema::default();
        for declaration in &declarations {
            schema.register(declaration)?;
        }
        for declaration in declarations {
            schema.apply(declaration)?;
        }
        schema.resolve_inheritance()?;
        Ok(schema)
    }

    fn register(&mut self, declaration: &typeql::schema::definable::Type) -> Result<(), String> {
        let label = declaration.label.ident.to_string();
        match declaration.kind {
            None => Ok(()),
            Some(token::Kind::Attribute) => {
                if self.attribute(&label).is_none() {
                    self.attributes.push(AttributeType { label, abstract_: false, supertype: None, value_type: None });
                }
                Ok(())
            }
            Some(kind @ (token::Kind::Entity | token::Kind::Relation)) => {
                let types = if kind == token::Kind::Relation { &mut self.relations } else { &mut self.entities };
                if !types.iter().any(|t| t.label == label) {
                    types.push(ObjectType {
                        label,
                        abstract_: false,
                        supertype: None,
                        owns: Vec::new(),
                        plays: Vec::new(),
                        relates: Vec::new(),
                    });
                }
                Ok(())
            }
            Some(token::Kind::Role) => Err(format!("unexpected top-level role declaration '{label}'")),
        }
    }

    fn apply(&mut self, declaration: typeql::schema::definable::Type) -> Result<(), String> {
        let label = declaration.label.ident.to_string();
        let abstract_ = declaration.annotations.iter().any(|a| matches!(a, Annotation::Abstract(_)));
        if let Some(attribute) = self.attributes.iter_mut().find(|a| a.label == label) {
            attribute.abstract_ |= abstract_;
            for capability in declaration.capabilities {
                apply_attribute_capability(attribute, capability)?;
            }
            return Ok(());
        }
        for (types, is_relation) in [(&mut self.entities, false), (&mut self.relations, true)] {
            if let Some(object) = types.iter_mut().find(|o| o.label == label) {
                object.abstract_ |= abstract_;
                for capability in declaration.capabilities {
                    apply_object_capability(object, capability, is_relation)?;
                }
                return Ok(());
            }
        }
        Err(format!("type '{label}' has no kinded declaration (entity/relation/attribute) in this schema"))
    }

    fn resolve_inheritance(&mut self) -> Result<(), String> {
        let declared: HashMap<String, AttributeType> =
            self.attributes.iter().map(|a| (a.label.clone(), a.clone())).collect();
        for attribute in &mut self.attributes {
            let mut visited = HashSet::new();
            let mut ancestor = attribute.supertype.clone();
            while let Some(label) = ancestor {
                if !visited.insert(label.clone()) {
                    return Err(format!("supertype cycle through '{label}'"));
                }
                let Some(parent) = declared.get(&label) else {
                    return Err(format!("'{}' extends unknown attribute type '{label}'", attribute.label));
                };
                attribute.value_type = attribute.value_type.or(parent.value_type);
                ancestor = parent.supertype.clone();
            }
        }

        for types in [&mut self.entities, &mut self.relations] {
            let by_label: HashMap<String, ObjectType> = types.iter().map(|o| (o.label.clone(), o.clone())).collect();
            for object in types.iter_mut() {
                // A specialised role is replaced by its specialisation everywhere below the
                // specialising relation.
                let mut specialised: HashSet<String> =
                    object.relates.iter().filter_map(|r| r.specialises.clone()).collect();
                let mut ancestors = Vec::new();
                let mut ancestor = object.supertype.clone();
                while let Some(label) = ancestor {
                    if ancestors.iter().any(|parent: &&ObjectType| parent.label == label) {
                        return Err(format!("supertype cycle through '{label}'"));
                    }
                    let Some(parent) = by_label.get(&label) else {
                        return Err(format!("'{}' extends unknown supertype '{label}'", object.label));
                    };
                    specialised.extend(parent.relates.iter().filter_map(|r| r.specialises.clone()));
                    ancestor = parent.supertype.clone();
                    ancestors.push(parent);
                }
                object.relates.retain(|r| !specialised.contains(&r.role));

                for parent in ancestors {
                    for owns in &parent.owns {
                        if !object.owns.contains(owns) {
                            object.owns.push(owns.clone());
                        }
                    }
                    for plays in &parent.plays {
                        if !object.plays.iter().any(|p| p.relation == plays.relation && p.role == plays.role) {
                            object.plays.push(plays.clone());
                        }
                    }
                    for relates in &parent.relates {
                        if !specialised.contains(&relates.role)
                            && !object.relates.iter().any(|r| r.role == relates.role)
                        {
                            object.relates.push(relates.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn attribute(&self, label: &str) -> Option<&AttributeType> {
        self.attributes.iter().find(|a| a.label == label)
    }

    pub(crate) fn object(&self, label: &str, is_relation: bool) -> Option<&ObjectType> {
        let types = if is_relation { &self.relations } else { &self.entities };
        types.iter().find(|o| o.label == label)
    }

    pub(crate) fn relation_ancestry(&self, label: &str) -> Vec<String> {
        let mut ancestry = Vec::new();
        let mut current = Some(label.to_owned());
        while let Some(label) = current {
            current = self.object(&label, true).and_then(|relation| relation.supertype.clone());
            ancestry.push(label);
        }
        ancestry
    }
}

fn apply_attribute_capability(attribute: &mut AttributeType, capability: Capability) -> Result<(), String> {
    match capability.base {
        CapabilityBase::Sub(sub) => attribute.supertype = Some(sub.supertype_label.ident.to_string()),
        CapabilityBase::ValueType(value_type) => {
            attribute.value_type = Some(parse_value_type(&value_type.value_type, &attribute.label)?)
        }
        other => {
            return Err(format!(
                "attribute '{}' declares '{}', which attributes cannot have (only sub and value)",
                attribute.label,
                capability_name(&other)
            ));
        }
    }
    Ok(())
}

fn apply_object_capability(object: &mut ObjectType, capability: Capability, is_relation: bool) -> Result<(), String> {
    match capability.base {
        CapabilityBase::Sub(sub) => object.supertype = Some(sub.supertype_label.ident.to_string()),
        CapabilityBase::Owns(owns) => {
            let attribute = type_ref_any_label(&owns.owned).ok_or_else(|| {
                format!(
                    "'{} owns {}': ordered (list) ownership is not implemented in TypeDB 3.x",
                    object.label, owns.owned
                )
            })?;
            object.owns.push(attribute);
        }
        CapabilityBase::Plays(plays) => object
            .plays
            .push(Plays { relation: plays.role.scope.ident.to_string(), role: plays.role.name.ident.to_string() }),
        CapabilityBase::Relates(relates) if is_relation => {
            let role = type_ref_any_label(&relates.related).ok_or_else(|| {
                format!(
                    "'{} relates {}': ordered (list) roles are not implemented in TypeDB 3.x",
                    object.label, relates.related
                )
            })?;
            let specialises = relates.specialised.as_ref().and_then(type_ref_any_label);
            object.relates.push(Relates { role, declared_by: object.label.clone(), specialises });
        }
        other => {
            return Err(format!(
                "'{}' declares '{}', which this kind cannot have",
                object.label,
                capability_name(&other)
            ));
        }
    }
    Ok(())
}

fn type_ref_any_label(type_ref: &TypeRefAny) -> Option<String> {
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
            Err(format!("attribute '{attribute}' uses struct value type '{}', which TypeDB 3.x has not", label.ident))
        }
    }
}

fn capability_name(capability: &CapabilityBase) -> &'static str {
    match capability {
        CapabilityBase::Sub(_) => "sub",
        CapabilityBase::Alias(_) => "alias",
        CapabilityBase::Owns(_) => "owns",
        CapabilityBase::Plays(_) => "plays",
        CapabilityBase::Relates(_) => "relates",
        CapabilityBase::ValueType(_) => "value",
    }
}
