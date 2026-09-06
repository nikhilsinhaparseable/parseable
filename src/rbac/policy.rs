/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use datafusion::{
    arrow::datatypes::{DataType, Schema},
    common::{Column, ScalarValue},
    logical_expr::{Expr, Like, LogicalPlan, Operator},
    prelude::lit,
};

use crate::{
    parseable::{DEFAULT_TENANT, PARSEABLE},
    utils::{ConditionConfig, Conditions, LogicalOperator, WhereConfigOperator},
};

use super::{
    Users,
    map::{SessionKey, read_user_groups, roles},
    role::{
        ParseableResourceType,
        model::{DefaultPrivilege, Role, RowPolicy},
    },
};

pub type MandatoryQueryFilters = HashMap<String, Expr>;

/// Stable within one process; sufficient to partition in-memory query caches by policy.
pub fn mandatory_filters_cache_identity(filters: &MandatoryQueryFilters) -> u64 {
    let mut rendered = filters
        .iter()
        .map(|(stream, filter)| format!("{stream}:{filter}"))
        .collect::<Vec<_>>();
    rendered.sort_unstable();
    let mut hasher = DefaultHasher::new();
    rendered.hash(&mut hasher);
    hasher.finish()
}

/// Reject explicit equality/IN values that a finite allow-list excludes or a deny-list removes.
/// Complex policies remain enforced as filters, but are not treated as statically enumerable.
pub fn validate_query_against_mandatory_filters(
    plan: &LogicalPlan,
    filters: &MandatoryQueryFilters,
) -> Result<(), String> {
    if filters.len() != 1 {
        return Ok(());
    }
    let Some(stream) = single_table_scan(plan) else {
        // Joins and plans without one unambiguous table are still filtered at every
        // scan, but are too ambiguous for static value rejection.
        return Ok(());
    };
    let Some(filter) = filters.get(stream) else {
        return Ok(());
    };
    let (allow, deny) = split_policy_filter(filter);
    let finite_allow = equality_union(allow);
    let mut denied = HashMap::<String, HashSet<String>>::new();
    if let Some(deny) = deny {
        let mut candidate = HashMap::new();
        if collect_equality_union(deny, &mut candidate) {
            denied = candidate;
        }
    }

    if let Some(rejection) = find_rejection_in_plan(plan, &finite_allow, &denied) {
        return Err(rejection.message());
    }
    Ok(())
}

enum PolicyRejection {
    NotAllowed { column: String, value: String },
    Denied { column: String, value: String },
}

impl PolicyRejection {
    fn message(self) -> String {
        match self {
            Self::NotAllowed { column, value } => {
                format!("unauthorized row-policy value: column '{column}' does not allow '{value}'")
            }
            Self::Denied { column, value } => {
                format!("unauthorized row-policy value: column '{column}' denies '{value}'")
            }
        }
    }
}

fn single_table_scan(plan: &LogicalPlan) -> Option<&str> {
    fn collect<'a>(plan: &'a LogicalPlan, tables: &mut Vec<&'a str>) {
        if let LogicalPlan::TableScan(scan) = plan {
            tables.push(scan.table_name.table());
        }
        for input in plan.inputs() {
            collect(input, tables);
        }
    }

    let mut tables = Vec::new();
    collect(plan, &mut tables);
    (tables.len() == 1).then_some(tables[0])
}

fn split_policy_filter(filter: &Expr) -> (&Expr, Option<&Expr>) {
    if let Expr::BinaryExpr(binary) = filter
        && binary.op == Operator::And
        && let Expr::IsNotTrue(deny) = binary.right.as_ref()
    {
        return (binary.left.as_ref(), Some(deny.as_ref()));
    }
    (filter, None)
}

fn equality_union(expression: &Expr) -> Option<(String, HashSet<String>)> {
    let mut values = HashMap::new();
    if !collect_equality_union(expression, &mut values) || values.len() != 1 {
        return None;
    }
    values.into_iter().next()
}

fn collect_equality_union(
    expression: &Expr,
    values: &mut HashMap<String, HashSet<String>>,
) -> bool {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_equality_union(binary.left.as_ref(), values)
                && collect_equality_union(binary.right.as_ref(), values)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if let Some((column, value)) =
                column_literal(binary.left.as_ref(), binary.right.as_ref())
                    .or_else(|| column_literal(binary.right.as_ref(), binary.left.as_ref()))
            {
                values.entry(column).or_default().insert(value);
                true
            } else {
                false
            }
        }
        Expr::Alias(alias) => collect_equality_union(alias.expr.as_ref(), values),
        _ => false,
    }
}

fn find_rejection_in_plan(
    plan: &LogicalPlan,
    finite_allow: &Option<(String, HashSet<String>)>,
    denied: &HashMap<String, HashSet<String>>,
) -> Option<PolicyRejection> {
    if let LogicalPlan::Filter(filter) = plan {
        let rejection = find_rejection_in_expr(&filter.predicate, finite_allow, denied);
        if rejection.is_some() {
            return rejection;
        }
    }
    for input in plan.inputs() {
        let rejection = find_rejection_in_plan(input, finite_allow, denied);
        if rejection.is_some() {
            return rejection;
        }
    }
    None
}

fn find_rejection_in_expr(
    expression: &Expr,
    finite_allow: &Option<(String, HashSet<String>)>,
    denied: &HashMap<String, HashSet<String>>,
) -> Option<PolicyRejection> {
    match expression {
        Expr::Alias(alias) => find_rejection_in_expr(&alias.expr, finite_allow, denied),
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            find_rejection_in_expr(&binary.left, finite_allow, denied)
                .or_else(|| find_rejection_in_expr(&binary.right, finite_allow, denied))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = find_rejection_in_expr(&binary.left, finite_allow, denied);
            let right = find_rejection_in_expr(&binary.right, finite_allow, denied);
            match (left, right) {
                (Some(rejection), Some(_)) => Some(rejection),
                _ => None,
            }
        }
        Expr::Not(_) => None,
        _ => {
            let (column, values) = requested_values(expression)?;
            reject_requested_values(column, &values, finite_allow, denied)
        }
    }
}

fn requested_values(expression: &Expr) -> Option<(String, Vec<String>)> {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (column, value) = column_literal(&binary.left, &binary.right)
                .or_else(|| column_literal(&binary.right, &binary.left))?;
            Some((column, vec![value]))
        }
        Expr::InList(list) if !list.negated => {
            let Expr::Column(column) = list.expr.as_ref() else {
                return None;
            };
            let values = list
                .list
                .iter()
                .map(|expression| match expression {
                    Expr::Literal(value, _) => Some(scalar_value_text(value)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            (!values.is_empty()).then(|| (column.name.clone(), values))
        }
        _ => None,
    }
}

fn reject_requested_values(
    column: String,
    values: &[String],
    finite_allow: &Option<(String, HashSet<String>)>,
    denied: &HashMap<String, HashSet<String>>,
) -> Option<PolicyRejection> {
    let allowed_values = finite_allow
        .as_ref()
        .filter(|(allow_column, _)| column == *allow_column)
        .map(|(_, allowed)| allowed);
    let allowed_candidates = values
        .iter()
        .filter(|value| allowed_values.is_none_or(|allowed| allowed.contains(*value)))
        .collect::<Vec<_>>();

    if allowed_candidates.is_empty() && allowed_values.is_some() {
        return Some(PolicyRejection::NotAllowed {
            column,
            value: values[0].clone(),
        });
    }

    if let Some(denied_values) = denied.get(&column)
        && allowed_candidates
            .iter()
            .all(|value| denied_values.contains(*value))
    {
        return Some(PolicyRejection::Denied {
            column,
            value: (*allowed_candidates[0]).clone(),
        });
    }

    None
}

fn column_literal(column: &Expr, value: &Expr) -> Option<(String, String)> {
    let Expr::Column(column) = column else {
        return None;
    };
    let Expr::Literal(value, _) = value else {
        return None;
    };
    Some((column.name.clone(), scalar_value_text(value)))
}

fn scalar_value_text(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => value.clone(),
        _ => value.to_string(),
    }
}

/// Validate row policies before persisting their containing role.
pub async fn validate_role_policy(role: &Role, tenant_id: &Option<String>) -> Result<(), String> {
    let policy_streams = role
        .privileges()
        .iter()
        .filter_map(|privilege| match privilege {
            DefaultPrivilege::Reader {
                resource: Some(ParseableResourceType::Stream(stream)),
                row_policy: Some(_),
            } => Some(stream),
            _ => None,
        })
        .collect::<HashSet<_>>();

    for stream in policy_streams {
        let has_unrestricted_privilege =
            role.privileges().iter().any(|privilege| match privilege {
                DefaultPrivilege::SuperAdmin
                | DefaultPrivilege::Admin
                | DefaultPrivilege::Editor => true,
                DefaultPrivilege::Writer { resource } => {
                    resource_matches_stream(resource.as_ref(), stream)
                }
                DefaultPrivilege::Reader {
                    resource,
                    row_policy: None,
                } => resource_matches_stream(resource.as_ref(), stream),
                _ => false,
            });
        if has_unrestricted_privilege {
            return Err(format!(
                "role grants both unrestricted and row-policy access to stream '{stream}'"
            ));
        }
    }

    for privilege in role.privileges() {
        let DefaultPrivilege::Reader {
            resource,
            row_policy: Some(policy),
        } = privilege
        else {
            continue;
        };

        let stream = match resource {
            Some(ParseableResourceType::Stream(stream)) if stream != "*" => stream,
            _ => {
                return Err(
                    "rowPolicy requires one explicit stream resource; global and wildcard resources are not supported"
                        .to_string(),
                );
            }
        };

        if policy.is_empty() {
            return Err("rowPolicy must contain at least one allow or deny group".to_string());
        }
        if !PARSEABLE.check_or_load_stream(stream, tenant_id).await {
            return Err(format!(
                "cannot validate rowPolicy: stream '{stream}' does not exist"
            ));
        }

        let schema = PARSEABLE
            .get_stream(stream, tenant_id)
            .map_err(|error| format!("cannot validate rowPolicy for stream '{stream}': {error}"))?
            .get_schema();
        compile_effective_policy(&[policy], schema.as_ref())?;
    }

    Ok(())
}

/// Resolve mandatory filters for the authenticated user's queried streams.
pub fn mandatory_filters_for_session(
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    streams: &[String],
) -> Result<MandatoryQueryFilters, String> {
    let (user_id, session_tenant) = Users
        .get_userid_from_session(session_key)
        .ok_or_else(|| "cannot resolve user for row-policy evaluation".to_string())?;
    let requested_tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    if session_tenant != requested_tenant {
        return Err("query tenant does not match authenticated tenant".to_string());
    }

    mandatory_filters(&user_id, tenant_id, streams)
}

/// Require Writer-or-higher access to every stream. Reader access is intentionally excluded.
pub fn require_writer_access_for_session(
    session_key: &SessionKey,
    tenant_id: &Option<String>,
    streams: &[String],
) -> Result<(), String> {
    let (user_id, session_tenant) = Users
        .get_userid_from_session(session_key)
        .ok_or_else(|| "cannot resolve user for alert authorization".to_string())?;
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    if session_tenant != tenant {
        return Err("alert tenant does not match authenticated tenant".to_string());
    }

    let role_names = effective_role_names(&user_id, tenant_id, tenant);
    let role_map = roles();
    let tenant_roles = role_map
        .get(tenant)
        .ok_or_else(|| format!("no roles loaded for tenant '{tenant}'"))?;
    let assigned_roles = role_names
        .iter()
        .filter_map(|role_name| tenant_roles.get(role_name))
        .collect::<Vec<_>>();

    for stream in streams.iter().collect::<HashSet<_>>() {
        let authorized = assigned_roles
            .iter()
            .any(|role| role_has_writer_access(role, stream));
        if !authorized {
            return Err(format!(
                "Writer access is required to create or modify an alert for stream '{stream}'"
            ));
        }
    }

    Ok(())
}

fn role_has_writer_access(role: &Role, stream: &str) -> bool {
    role.privileges().iter().any(|privilege| match privilege {
        DefaultPrivilege::SuperAdmin | DefaultPrivilege::Admin | DefaultPrivilege::Editor => true,
        DefaultPrivilege::Writer { resource } => resource_matches_stream(resource.as_ref(), stream),
        _ => false,
    })
}

/// Resolve mandatory filters for an explicitly identified user when no HTTP session is available.
pub fn mandatory_filters_for_user(
    user_id: &str,
    tenant_id: &Option<String>,
    streams: &[String],
) -> Result<MandatoryQueryFilters, String> {
    if Users.get_user(user_id, tenant_id).is_none() {
        return Err(format!("row-policy user '{user_id}' does not exist"));
    }
    mandatory_filters(user_id, tenant_id, streams)
}

/// Used for mixed-version cluster requests that predate per-request policy identity.
/// Unknown identity is safe only when no role can apply a row policy to this stream.
pub fn stream_has_any_row_policy(tenant_id: &Option<String>, stream: &str) -> Result<bool, String> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let role_map = roles();
    let tenant_roles = role_map
        .get(tenant)
        .ok_or_else(|| format!("no roles loaded for tenant '{tenant}'"))?;

    Ok(tenant_roles.values().any(|role| {
        role.privileges().iter().any(|privilege| {
            matches!(
                privilege,
                DefaultPrivilege::Reader {
                    resource,
                    row_policy: Some(_),
                } if resource_matches_stream(resource.as_ref(), stream)
            )
        })
    }))
}

fn mandatory_filters(
    user_id: &str,
    tenant_id: &Option<String>,
    streams: &[String],
) -> Result<MandatoryQueryFilters, String> {
    let tenant = tenant_id.as_deref().unwrap_or(DEFAULT_TENANT);
    let role_names = effective_role_names(user_id, tenant_id, tenant);
    let role_map = roles();
    let tenant_roles = role_map
        .get(tenant)
        .ok_or_else(|| format!("no roles loaded for tenant '{tenant}'"))?;
    let assigned_roles: Vec<&Role> = role_names
        .iter()
        .filter_map(|role_name| tenant_roles.get(role_name))
        .collect();

    if assigned_roles.iter().any(|role| {
        role.privileges()
            .iter()
            .any(|privilege| matches!(privilege, DefaultPrivilege::SuperAdmin))
    }) {
        return Ok(HashMap::new());
    }

    let mut filters = HashMap::new();
    for stream in streams.iter().collect::<HashSet<_>>() {
        let is_internal = PARSEABLE
            .get_stream(stream, tenant_id)
            .is_ok_and(|stream| stream.get_stream_type() == crate::storage::StreamType::Internal);
        let mut unrestricted = false;
        let mut has_query_access = false;
        let mut policies = Vec::new();

        for role in &assigned_roles {
            for privilege in role.privileges() {
                if is_internal && privilege_grants_internal_query(privilege) {
                    has_query_access = true;
                    unrestricted = true;
                    continue;
                }
                match privilege {
                    DefaultPrivilege::Admin | DefaultPrivilege::Editor => {
                        has_query_access = true;
                        unrestricted = true;
                    }
                    DefaultPrivilege::Writer { resource }
                        if resource_matches_stream(resource.as_ref(), stream) =>
                    {
                        has_query_access = true;
                        unrestricted = true;
                    }
                    DefaultPrivilege::Reader {
                        resource,
                        row_policy,
                    } if resource_matches_stream(resource.as_ref(), stream) => {
                        has_query_access = true;
                        if let Some(policy) = row_policy {
                            policies.push(policy);
                        } else {
                            unrestricted = true;
                        }
                    }
                    _ => {}
                }
            }
        }

        if !has_query_access {
            return Err(format!(
                "user '{user_id}' no longer has query access to stream '{stream}'"
            ));
        }

        if !requires_row_filter(&policies, unrestricted) {
            continue;
        }

        let schema = PARSEABLE
            .get_stream(stream, tenant_id)
            .map_err(|error| format!("cannot load schema for stream '{stream}': {error}"))?
            .get_schema();
        filters.insert(
            (*stream).clone(),
            compile_effective_policy(&policies, schema.as_ref())?,
        );
    }

    Ok(filters)
}

fn requires_row_filter(policies: &[&RowPolicy], unrestricted: bool) -> bool {
    !unrestricted && !policies.is_empty()
}

fn privilege_grants_internal_query(privilege: &DefaultPrivilege) -> bool {
    match privilege {
        DefaultPrivilege::SuperAdmin | DefaultPrivilege::Admin | DefaultPrivilege::Editor => true,
        DefaultPrivilege::Writer { resource } | DefaultPrivilege::Reader { resource, .. } => {
            matches!(
                resource,
                None | Some(ParseableResourceType::All) | Some(ParseableResourceType::Stream(_))
            )
        }
        DefaultPrivilege::Ingestor { .. } => false,
    }
}

fn effective_role_names(
    user_id: &str,
    tenant_id: &Option<String>,
    tenant: &str,
) -> HashSet<String> {
    let mut role_names: HashSet<String> = Users.get_role(user_id, tenant_id).into_iter().collect();
    let group_names = Users.get_user_groups(user_id, tenant_id);
    let groups = read_user_groups();
    if let Some(tenant_groups) = groups.get(tenant) {
        for group_name in group_names {
            if let Some(group) = tenant_groups.get(&group_name) {
                role_names.extend(group.roles.iter().cloned());
            }
        }
    }
    role_names
}

fn resource_matches_stream(resource: Option<&ParseableResourceType>, stream: &str) -> bool {
    match resource {
        None | Some(ParseableResourceType::All) => true,
        Some(ParseableResourceType::Stream(candidate)) => candidate == stream || candidate == "*",
        Some(ParseableResourceType::Llm(_)) => false,
    }
}

fn compile_effective_policy(policies: &[&RowPolicy], schema: &Schema) -> Result<Expr, String> {
    policies
        .iter()
        .map(|policy| compile_policy(policy, schema))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .reduce(Expr::or)
        .ok_or_else(|| "rowPolicy has no effective policy".to_string())
}

fn compile_policy(policy: &RowPolicy, schema: &Schema) -> Result<Expr, String> {
    let allow = if policy.allow.is_empty() {
        lit(true)
    } else {
        policy
            .allow
            .iter()
            .map(|conditions| compile_conditions(conditions, schema))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .reduce(Expr::or)
            .ok_or_else(|| "rowPolicy has no effective allow condition".to_string())?
    };

    let deny = policy
        .deny
        .iter()
        .map(|conditions| compile_conditions(conditions, schema))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .reduce(Expr::or);

    Ok(match deny {
        Some(deny) => allow.and(deny.is_not_true()),
        None => allow,
    })
}

fn compile_conditions(conditions: &Conditions, schema: &Schema) -> Result<Expr, String> {
    let mut expressions = conditions
        .condition_config
        .iter()
        .map(|condition| compile_condition(condition, schema))
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(groups) = &conditions.groups {
        expressions.extend(
            groups
                .iter()
                .map(|group| compile_conditions(group, schema))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    let operator = conditions
        .operator
        .as_ref()
        .unwrap_or(&LogicalOperator::And);
    match operator {
        LogicalOperator::And => expressions.into_iter().reduce(Expr::and),
        LogicalOperator::Or => expressions.into_iter().reduce(Expr::or),
    }
    .ok_or_else(|| "rowPolicy condition group cannot be empty".to_string())
}

fn compile_condition(condition: &ConditionConfig, schema: &Schema) -> Result<Expr, String> {
    if condition.column.trim().is_empty() {
        return Err("rowPolicy condition column cannot be empty".to_string());
    }

    let field = schema
        .field_with_name(&condition.column)
        .map_err(|_| format!("rowPolicy references unknown column '{}'", condition.column))?;
    let column = Expr::Column(Column::new_unqualified(condition.column.clone()));

    match condition.operator {
        WhereConfigOperator::IsNull | WhereConfigOperator::IsNotNull => {
            if condition
                .value
                .as_ref()
                .is_some_and(|value| !value.is_empty())
            {
                return Err(format!(
                    "rowPolicy condition for column '{}' must not have a value with operator '{}'",
                    condition.column, condition.operator
                ));
            }
            if condition.operator == WhereConfigOperator::IsNull {
                Ok(column.is_null())
            } else {
                Ok(column.is_not_null())
            }
        }
        WhereConfigOperator::Contains
        | WhereConfigOperator::ILike
        | WhereConfigOperator::BeginsWith
        | WhereConfigOperator::EndsWith
        | WhereConfigOperator::DoesNotContain
        | WhereConfigOperator::DoesNotBeginWith
        | WhereConfigOperator::DoesNotEndWith => {
            if !is_string_type(field.data_type()) {
                return Err(format!(
                    "operator '{}' requires a string column, but '{}' has type {}",
                    condition.operator,
                    condition.column,
                    field.data_type()
                ));
            }
            compile_string_condition(column, condition)
        }
        _ => {
            let raw_value = condition.value.clone().ok_or_else(|| {
                format!(
                    "rowPolicy condition for '{}' requires a value",
                    condition.column
                )
            })?;
            let scalar =
                ScalarValue::try_from_string(raw_value, field.data_type()).map_err(|error| {
                    format!(
                        "invalid value for rowPolicy column '{}' of type {}: {error}",
                        condition.column,
                        field.data_type()
                    )
                })?;
            let value = Expr::Literal(scalar, None);
            match condition.operator {
                WhereConfigOperator::Equal => Ok(column.eq(value)),
                WhereConfigOperator::NotEqual => Ok(column.not_eq(value)),
                WhereConfigOperator::LessThan => Ok(column.lt(value)),
                WhereConfigOperator::GreaterThan => Ok(column.gt(value)),
                WhereConfigOperator::LessThanOrEqual => Ok(column.lt_eq(value)),
                WhereConfigOperator::GreaterThanOrEqual => Ok(column.gt_eq(value)),
                _ => unreachable!("string and null operators handled above"),
            }
        }
    }
}

fn compile_string_condition(column: Expr, condition: &ConditionConfig) -> Result<Expr, String> {
    let value = condition.value.as_deref().ok_or_else(|| {
        format!(
            "rowPolicy condition for '{}' requires a value",
            condition.column
        )
    })?;
    let escaped = escape_like(value);
    let (pattern, negated, case_insensitive) = match condition.operator {
        WhereConfigOperator::Contains => (format!("%{escaped}%"), false, false),
        WhereConfigOperator::ILike => (format!("%{escaped}%"), false, true),
        WhereConfigOperator::BeginsWith => (format!("{escaped}%"), false, false),
        WhereConfigOperator::EndsWith => (format!("%{escaped}"), false, false),
        WhereConfigOperator::DoesNotContain => (format!("%{escaped}%"), true, false),
        WhereConfigOperator::DoesNotBeginWith => (format!("{escaped}%"), true, false),
        WhereConfigOperator::DoesNotEndWith => (format!("%{escaped}"), true, false),
        _ => unreachable!("only string operators reach this function"),
    };

    Ok(Expr::Like(Like::new(
        negated,
        Box::new(column),
        Box::new(lit(pattern)),
        Some('\\'),
        case_insensitive,
    )))
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::{datatypes::Field, record_batch::RecordBatch};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn condition(column: &str, operator: WhereConfigOperator, value: Option<&str>) -> Conditions {
        Conditions {
            operator: Some(LogicalOperator::And),
            condition_config: vec![ConditionConfig {
                column: column.to_string(),
                operator,
                value: value.map(str::to_string),
                column_type: Some("string".to_string()),
            }],
            groups: None,
        }
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("env", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("attempts", DataType::Int64, true),
        ])
    }

    async fn query_plan(sql: &str, tables: &[&str]) -> LogicalPlan {
        let context = SessionContext::new();
        for table_name in tables {
            let schema = Arc::new(schema());
            let table =
                MemTable::try_new(schema.clone(), vec![vec![RecordBatch::new_empty(schema)]])
                    .unwrap();
            context
                .register_table(*table_name, Arc::new(table))
                .unwrap();
        }
        context.state().create_logical_plan(sql).await.unwrap()
    }

    #[test]
    fn allow_groups_are_combined_with_or() {
        let policy = RowPolicy {
            allow: vec![
                condition("env", WhereConfigOperator::Equal, Some("staging")),
                condition("env", WhereConfigOperator::Equal, Some("prod")),
            ],
            deny: vec![],
        };

        let rendered = compile_effective_policy(&[&policy], &schema())
            .unwrap()
            .to_string();
        assert!(rendered.contains("env = Utf8(\"staging\")"));
        assert!(rendered.contains("OR"));
        assert!(rendered.contains("env = Utf8(\"prod\")"));
    }

    #[test]
    fn deny_uses_is_not_true_so_null_rows_survive() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![condition(
                "env",
                WhereConfigOperator::Equal,
                Some("staging"),
            )],
        };

        let expression = compile_effective_policy(&[&policy], &schema()).unwrap();
        assert!(expression.to_string().contains("IS NOT TRUE"));
    }

    #[test]
    fn unrestricted_privilege_wins_over_other_row_policies() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![condition(
                "env",
                WhereConfigOperator::Equal,
                Some("staging"),
            )],
        };

        assert!(!requires_row_filter(&[&policy], true));
        assert!(requires_row_filter(&[&policy], false));
    }

    #[test]
    fn multiple_deny_groups_are_combined_with_or() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![
                condition("env", WhereConfigOperator::Equal, Some("prod")),
                condition("env", WhereConfigOperator::Equal, Some("internal")),
            ],
        };

        let rendered = compile_effective_policy(&[&policy], &schema())
            .unwrap()
            .to_string();
        assert!(rendered.contains("env = Utf8(\"prod\")"));
        assert!(rendered.contains("OR"));
        assert!(rendered.contains("env = Utf8(\"internal\")"));
        assert!(rendered.contains("IS NOT TRUE"));
    }

    #[test]
    fn policies_from_multiple_roles_are_combined_as_independent_grants() {
        let deny_staging = RowPolicy {
            allow: vec![],
            deny: vec![condition(
                "env",
                WhereConfigOperator::Equal,
                Some("staging"),
            )],
        };
        let allow_staging = RowPolicy {
            allow: vec![condition(
                "env",
                WhereConfigOperator::Equal,
                Some("staging"),
            )],
            deny: vec![],
        };

        let rendered = compile_effective_policy(&[&deny_staging, &allow_staging], &schema())
            .unwrap()
            .to_string();

        assert!(rendered.contains("IS NOT TRUE"));
        assert!(rendered.contains(" OR "));
    }

    #[test]
    fn deny_can_subtract_from_allow() {
        let policy = RowPolicy {
            allow: vec![Conditions {
                operator: Some(LogicalOperator::Or),
                condition_config: vec![
                    condition("level", WhereConfigOperator::Equal, Some("info"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                    condition("level", WhereConfigOperator::Equal, Some("debug"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                ],
                groups: None,
            }],
            deny: vec![Conditions {
                operator: Some(LogicalOperator::Or),
                condition_config: vec![
                    condition("level", WhereConfigOperator::Equal, Some("debug"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                    condition("level", WhereConfigOperator::Equal, Some("warn"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                ],
                groups: None,
            }],
        };

        let rendered = compile_effective_policy(&[&policy], &schema())
            .unwrap()
            .to_string();
        assert!(rendered.contains("level = Utf8(\"debug\")"));
        assert!(rendered.contains("IS NOT TRUE"));
    }

    #[test]
    fn unknown_columns_fail_closed() {
        let policy = RowPolicy {
            allow: vec![condition(
                "missing",
                WhereConfigOperator::Equal,
                Some("value"),
            )],
            deny: vec![],
        };

        let error = compile_effective_policy(&[&policy], &schema()).unwrap_err();
        assert!(error.contains("unknown column 'missing'"));
    }

    #[test]
    fn values_are_parsed_using_stream_column_type() {
        let policy = RowPolicy {
            allow: vec![condition(
                "attempts",
                WhereConfigOperator::Equal,
                Some("not-a-number"),
            )],
            deny: vec![],
        };

        let error = compile_effective_policy(&[&policy], &schema()).unwrap_err();
        assert!(error.contains("column 'attempts' of type Int64"));
    }

    #[test]
    fn empty_policy_is_rejected() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![],
        };
        assert!(policy.is_empty());
    }

    #[test]
    fn reader_access_is_not_writer_access() {
        let role = Role::create_user_role(vec![DefaultPrivilege::Reader {
            resource: Some(ParseableResourceType::Stream("logs".to_string())),
            row_policy: None,
        }]);

        assert!(!role_has_writer_access(&role, "logs"));
    }

    #[test]
    fn writer_access_is_scoped_to_its_stream() {
        let role = Role::create_user_role(vec![DefaultPrivilege::Writer {
            resource: Some(ParseableResourceType::Stream("logs".to_string())),
        }]);

        assert!(role_has_writer_access(&role, "logs"));
        assert!(!role_has_writer_access(&role, "metrics"));
    }

    #[test]
    fn editor_has_writer_access_to_every_stream() {
        let role = Role::create_user_role(vec![DefaultPrivilege::Editor]);

        assert!(role_has_writer_access(&role, "logs"));
        assert!(role_has_writer_access(&role, "metrics"));
    }

    #[test]
    fn stream_reader_can_query_internal_streams_without_row_filter() {
        let privilege = DefaultPrivilege::Reader {
            resource: Some(ParseableResourceType::Stream("logs".to_string())),
            row_policy: Some(RowPolicy {
                allow: vec![condition(
                    "env",
                    WhereConfigOperator::Equal,
                    Some("staging"),
                )],
                deny: vec![],
            }),
        };

        assert!(privilege_grants_internal_query(&privilege));
    }

    #[test]
    fn llm_reader_cannot_query_internal_streams() {
        let privilege = DefaultPrivilege::Reader {
            resource: Some(ParseableResourceType::Llm("assistant".to_string())),
            row_policy: None,
        };

        assert!(!privilege_grants_internal_query(&privilege));
    }

    #[tokio::test]
    async fn wildcard_policy_resource_is_rejected() {
        let role = Role::create_user_role(vec![DefaultPrivilege::Reader {
            resource: Some(ParseableResourceType::Stream("*".to_string())),
            row_policy: Some(RowPolicy {
                allow: vec![condition(
                    "env",
                    WhereConfigOperator::Equal,
                    Some("staging"),
                )],
                deny: vec![],
            }),
        }]);

        let error = validate_role_policy(&role, &None).await.unwrap_err();
        assert!(error.contains("explicit stream resource"));
    }

    #[tokio::test]
    async fn same_role_cannot_mix_unrestricted_and_filtered_reader() {
        let role = Role::create_user_role(vec![
            DefaultPrivilege::Reader {
                resource: Some(ParseableResourceType::Stream("logs".to_string())),
                row_policy: None,
            },
            DefaultPrivilege::Reader {
                resource: Some(ParseableResourceType::Stream("logs".to_string())),
                row_policy: Some(RowPolicy {
                    allow: vec![condition(
                        "env",
                        WhereConfigOperator::Equal,
                        Some("staging"),
                    )],
                    deny: vec![],
                }),
            },
        ]);

        let error = validate_role_policy(&role, &None).await.unwrap_err();
        assert!(error.contains("both unrestricted and row-policy access"));
    }

    #[tokio::test]
    async fn explicit_value_outside_allow_list_is_rejected() {
        let policy = RowPolicy {
            allow: vec![condition("level", WhereConfigOperator::Equal, Some("info"))],
            deny: vec![],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan("SELECT * FROM logs WHERE level = 'warn'", &["logs"]).await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        let error = validate_query_against_mandatory_filters(&plan, &filters).unwrap_err();
        assert!(error.contains("column 'level' does not allow 'warn'"));
    }

    #[tokio::test]
    async fn explicit_denied_value_is_rejected() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![condition(
                "level",
                WhereConfigOperator::Equal,
                Some("debug"),
            )],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan("SELECT * FROM logs WHERE level IN ('debug')", &["logs"]).await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        let error = validate_query_against_mandatory_filters(&plan, &filters).unwrap_err();
        assert!(error.contains("column 'level' denies 'debug'"));
    }

    #[tokio::test]
    async fn mixed_in_list_with_visible_value_is_allowed() {
        let policy = RowPolicy {
            allow: vec![condition("env", WhereConfigOperator::Equal, Some("prod"))],
            deny: vec![],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan(
            "SELECT * FROM logs WHERE env IN ('prod', 'staging')",
            &["logs"],
        )
        .await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        validate_query_against_mandatory_filters(&plan, &filters).unwrap();
    }

    #[tokio::test]
    async fn negated_denied_value_is_not_rejected() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![condition("env", WhereConfigOperator::Equal, Some("prod"))],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan("SELECT * FROM logs WHERE NOT (env = 'prod')", &["logs"]).await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        validate_query_against_mandatory_filters(&plan, &filters).unwrap();
    }

    #[tokio::test]
    async fn visible_or_branch_avoids_false_rejection() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![condition(
                "env",
                WhereConfigOperator::Equal,
                Some("staging"),
            )],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan(
            "SELECT * FROM logs WHERE env = 'staging' OR level = 'api'",
            &["logs"],
        )
        .await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        validate_query_against_mandatory_filters(&plan, &filters).unwrap();
    }

    #[tokio::test]
    async fn or_is_rejected_when_every_branch_is_denied() {
        let policy = RowPolicy {
            allow: vec![],
            deny: vec![Conditions {
                operator: Some(LogicalOperator::Or),
                condition_config: vec![
                    condition("env", WhereConfigOperator::Equal, Some("staging"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                    condition("level", WhereConfigOperator::Equal, Some("api"))
                        .condition_config
                        .into_iter()
                        .next()
                        .unwrap(),
                ],
                groups: None,
            }],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan(
            "SELECT * FROM logs WHERE env = 'staging' OR level = 'api'",
            &["logs"],
        )
        .await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        let error = validate_query_against_mandatory_filters(&plan, &filters).unwrap_err();
        assert!(error.contains("denies"));
    }

    #[tokio::test]
    async fn predicates_on_another_joined_table_are_not_rejected() {
        let policy = RowPolicy {
            allow: vec![condition("env", WhereConfigOperator::Equal, Some("prod"))],
            deny: vec![],
        };
        let filter = compile_effective_policy(&[&policy], &schema()).unwrap();
        let plan = query_plan(
            "SELECT * FROM logs JOIN other ON logs.level = other.level WHERE other.env = 'staging'",
            &["logs", "other"],
        )
        .await;
        let filters = HashMap::from([("logs".to_string(), filter)]);

        validate_query_against_mandatory_filters(&plan, &filters).unwrap();
    }
}
