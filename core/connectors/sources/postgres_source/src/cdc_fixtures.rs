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

// Real `test_decoding` output rows, captured from
// pg_logical_slot_get_changes() on postgres:16 with the test_decoding
// plugin (BEGIN/COMMIT rows omitted, parser never sees them).

pub(crate) const INSERT_SINGLE_ROW_ALL_TYPES: &str = r#"table public.probe_events: INSERT: id[integer]:2 name[text]:'alice' note[text]:'first note' amount[numeric]:12.50 active[boolean]:true tags[text[]]:'{a,b}' payload[jsonb]:'{"k": 1}' created_at[timestamp with time zone]:'2026-07-05 17:58:23.202192+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_WITH_NULLS: &str = r#"table public.probe_events: INSERT: id[integer]:3 name[text]:'bob' note[text]:null amount[numeric]:null active[boolean]:null tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:23.924391+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_MULTI_ROW_SINGLE_STATEMENT: [&str; 2] = [
    r#"table public.probe_events: INSERT: id[integer]:4 name[text]:'carol' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:24.628431+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#,
    r#"table public.probe_events: INSERT: id[integer]:5 name[text]:'dave' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:24.628431+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#,
];

pub(crate) const INSERT_MULTI_STATEMENT_ONE_TRANSACTION: [&str; 2] = [
    r#"table public.probe_events: INSERT: id[integer]:6 name[text]:'eve' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:25.344503+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#,
    r#"table public.probe_events: INSERT: id[integer]:7 name[text]:'frank' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:25.344503+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#,
];

pub(crate) const UPDATE_FULL_ROW: &str = r#"table public.probe_events: UPDATE: id[integer]:2 name[text]:'alice2' note[text]:'updated' amount[numeric]:99.99 active[boolean]:false tags[text[]]:'{a,b}' payload[jsonb]:'{"k": 1}' created_at[timestamp with time zone]:'2026-07-05 17:58:23.202192+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const UPDATE_SINGLE_COLUMN: &str = r#"table public.probe_events: UPDATE: id[integer]:3 name[text]:'bob' note[text]:'only note changed' amount[numeric]:null active[boolean]:null tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:23.924391+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const UPDATE_TO_NULL: &str = r#"table public.probe_events: UPDATE: id[integer]:2 name[text]:'alice2' note[text]:null amount[numeric]:99.99 active[boolean]:false tags[text[]]:'{a,b}' payload[jsonb]:'{"k": 1}' created_at[timestamp with time zone]:'2026-07-05 17:58:23.202192+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

// PK change: test_decoding emits the pre-update key separately from the
// post-update tuple, `old-key: ... new-tuple: ...`, instead of the plain
// `UPDATE: <cols>` shape every other row here uses.
pub(crate) const UPDATE_PRIMARY_KEY: &str = r#"table public.probe_events: UPDATE: old-key: id[integer]:4 new-tuple: id[integer]:1004 name[text]:'carol' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:24.628431+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

// DELETE only carries replica-identity columns (here, just the PK), not
// the full row.
pub(crate) const DELETE_ROW: &str = "table public.probe_events: DELETE: id[integer]:5";

pub(crate) const DELETE_MULTIPLE_ROWS: [&str; 2] = [
    "table public.probe_events: DELETE: id[integer]:6",
    "table public.probe_events: DELETE: id[integer]:7",
];

pub(crate) const TRUNCATE_TABLE: &str = "table public.probe_events: TRUNCATE: (no-flags)";

// Value contains a literal embedded newline and an escaped quote (`''`) -
// the row spans two lines in the corpus capture.
pub(crate) const UNICODE_AND_SPECIAL_CHARS: &str = "table public.probe_events: INSERT: id[integer]:9 name[text]:'unicode' note[text]:'emoji \u{1F680} quote'' backslash\\ newline\nend' amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:31.874185+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null";

pub(crate) const INSERT_EXTENDED_TYPES_NORMAL_VALUES: &str = r#"table public.probe_events: INSERT: id[integer]:10 name[text]:'extended' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:32.595761+00' small_int[smallint]:42 big_int[bigint]:9000000000 real_val[real]:3.14 double_val[double precision]:2.718281828 numeric_val[numeric]:123.456789 uuid_val[uuid]:'11111111-1111-1111-1111-111111111111' bytea_val[bytea]:'\xdeadbeef' date_val[date]:'2024-01-15' time_val[time without time zone]:'13:45:30' interval_val[interval]:'2 days 03:00:00' int_array[integer[]]:'{1,2,3}' char_val[character]:'ab        ' varchar_val[character varying]:'ab'"#;

pub(crate) const INSERT_NUMERIC_NAN: &str = r#"table public.probe_events: INSERT: id[integer]:11 name[text]:'nan_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:33.30327+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:NaN double_val[double precision]:NaN numeric_val[numeric]:NaN uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_NUMERIC_INFINITY: &str = r#"table public.probe_events: INSERT: id[integer]:12 name[text]:'inf_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:34.023511+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:Infinity double_val[double precision]:-Infinity numeric_val[numeric]:Infinity uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_NEGATIVE_AND_BOUNDARY_NUMBERS: &str = r#"table public.probe_events: INSERT: id[integer]:13 name[text]:'neg_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:34.743797+00' small_int[smallint]:-32768 big_int[bigint]:-9223372036854775808 real_val[real]:-3.14 double_val[double precision]:-2.71828 numeric_val[numeric]:-999999.9999 uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_MAX_BOUNDARY_NUMBERS: &str = r#"table public.probe_events: INSERT: id[integer]:14 name[text]:'max_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:35.473792+00' small_int[smallint]:32767 big_int[bigint]:9223372036854775807 real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_NEGATIVE_ZERO_FLOAT: &str = r#"table public.probe_events: INSERT: id[integer]:15 name[text]:'negzero_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:36.198193+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:-0 double_val[double precision]:-0 numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_EMPTY_STRING_VS_NULL: [&str; 2] = [
    r#"table public.probe_events: INSERT: id[integer]:16 name[text]:'empty_string_row' note[text]:'' amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:36.931897+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:''"#,
    r#"table public.probe_events: INSERT: id[integer]:17 name[text]:'null_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:36.931897+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#,
];

pub(crate) const INSERT_ARRAY_WITH_NULL_ELEMENT: &str = r#"table public.probe_events: INSERT: id[integer]:18 name[text]:'array_null_elem_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:37.658172+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:'{1,2,NULL,4}' char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const INSERT_EMPTY_ARRAY: &str = r#"table public.probe_events: INSERT: id[integer]:19 name[text]:'empty_array_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:38.384294+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:'{}' char_val[character]:null varchar_val[character varying]:null"#;

pub(crate) const UPDATE_ARRAY_COLUMN: &str = r#"table public.probe_events: UPDATE: id[integer]:10 name[text]:'extended' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:32.595761+00' small_int[smallint]:42 big_int[bigint]:9000000000 real_val[real]:3.14 double_val[double precision]:2.718281828 numeric_val[numeric]:123.456789 uuid_val[uuid]:'11111111-1111-1111-1111-111111111111' bytea_val[bytea]:'\xdeadbeef' date_val[date]:'2024-01-15' time_val[time without time zone]:'13:45:30' interval_val[interval]:'2 days 03:00:00' int_array[integer[]]:'{9,8,7}' char_val[character]:'ab        ' varchar_val[character varying]:'ab'"#;

pub(crate) const INSERT_CHAR_PADDING_VS_VARCHAR: &str = r#"table public.probe_events: INSERT: id[integer]:20 name[text]:'padding_row' note[text]:null amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:null created_at[timestamp with time zone]:'2026-07-05 17:58:39.824702+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:'ab        ' varchar_val[character varying]:'ab'"#;

pub(crate) const INSERT_QUOTED_MIXED_CASE_COLUMN: &str = r#"table public.probe_events: INSERT: id[integer]:21 "createdAt"[timestamp with time zone]:'2026-07-05 17:58:40.000000+00' "user"[text]:'quoted_row'"#;

pub(crate) const UPDATE_UNCHANGED_TOAST_COLUMN: &str = r#"table public.probe_events: UPDATE: id[integer]:22 name[text]:'toast_row' note[text]:unchanged-toast-datum amount[numeric]:null active[boolean]:true tags[text[]]:null payload[jsonb]:unchanged-toast-datum created_at[timestamp with time zone]:'2026-07-05 17:58:41.000000+00' small_int[smallint]:null big_int[bigint]:null real_val[real]:null double_val[double precision]:null numeric_val[numeric]:null uuid_val[uuid]:null bytea_val[bytea]:null date_val[date]:null time_val[time without time zone]:null interval_val[interval]:null int_array[integer[]]:null char_val[character]:null varchar_val[character varying]:null"#;
