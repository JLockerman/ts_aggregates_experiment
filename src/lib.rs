use std::{
    mem::size_of,
    ptr::null_mut,
    slice,
};

use flat_serialize::{FlatSerialize, WrapErr};

use pgx::*;
use pg_sys::{Datum, TimestampTz};
use serde::{Serialize, Deserialize};

mod util;

pg_module_magic!();

#[pg_extern]
fn time_aggregate_trans(
    state: Option<Internal<TimebucketAggregateBuilder>>,
    time: TimestampTz,
    value: Option<AnyElement>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TimebucketAggregateBuilder>> {
    let mut mctx = null_mut();
    unsafe {
        pg_sys::AggCheckCallContext(fcinfo, &mut mctx)
    };
    let prev_ctx = unsafe { pg_sys::CurrentMemoryContext };
    unsafe { pg_sys::CurrentMemoryContext = mctx };
    let state = guard(|| {
        let mut state = match (state, &value) {
            (None, None) => return None,
            (Some(state), _) => state,
            (None, Some(value)) => {
                let state = TimebucketAggregateBuilder::new(value.oid());
                Internal(PgBox::from_rust(Box::leak(Box::new(state))))
            },
        };

        state.0.push(time, value.map(|v| v.datum()));

        Some(state)
    });
    unsafe { pg_sys::CurrentMemoryContext = prev_ctx }
    state
}

#[pg_extern]
fn time_aggregate_final<'input>(
    state: Option<Internal<TimebucketAggregateBuilder>>, fcinfo: pg_sys::FunctionCallInfo,
) -> Option<TimebucketAggregate<'input>> {
    let mut mctx = null_mut();
    unsafe {
        pg_sys::AggCheckCallContext(fcinfo, &mut mctx)
    };
    let prev_ctx = unsafe { pg_sys::CurrentMemoryContext };
    unsafe { pg_sys::CurrentMemoryContext = mctx };
    // TODO an empty builder should lead to an empty agg, not a NULL one
    let agg = guard(|| state.map(|mut state| state.0.build()));
    unsafe { pg_sys::CurrentMemoryContext = prev_ctx }
    agg
}

#[pg_extern]
fn time_aggregate_nop<'input>(input: Option<TimebucketAggregate<'input>>)
-> Option<TimebucketAggregate<'input>> {
    input
}

// #[pg_extern]
// fn time_bucket_sample<'input>(
//     input: Option<TimebucketAggregate<'input>>,
//     width: PgBox<pg_sys::Interval>,
// ) -> Option<TimebucketAggregate<'input>> {
//     let input = match input {
//         None => return None,
//         Some(input) => input,
//     };
//     let mut builder = TimebucketAggregateBuilder::with_capacity(input.times.len(), input.typ);
//     let mut times = input.times.into_iter();
//     let mut idx = 0;
//     todo!()
//     // width.time
//     // input
// }

#[pg_extern]
fn locf() -> AggregatePipelineElement {
    AggregatePipelineElement{ inner: PipelineElement::Locf }
}

#[pg_extern]
fn sample(width: PgBox<pg_sys::Interval>,) -> AggregatePipelineElement {
    AggregatePipelineElement{ inner: PipelineElement::Sample{width: width.time} }
}

#[pg_extern]
fn interpolate() -> AggregatePipelineElement {
    AggregatePipelineElement{ inner: PipelineElement::Interpolate{min: None, max: None} }
}

#[pg_extern]
fn aggregate_do_pipeline<'input>(
    input: Option<TimebucketAggregate<'input>>,
    operations: AggregatePipeline,
) -> Option<TimebucketAggregate<'input>> {
    input.map(|mut aggregate| {
        for operation in operations.pipeline {
            aggregate = match operation {
                PipelineElement::Sample{width} => do_sample(aggregate, width),
                PipelineElement::Locf => do_locf(aggregate),
                PipelineElement::Interpolate{..} => do_linear_interpolate(aggregate),
            }
        }
        aggregate
    })
}

#[pg_extern]
fn aggregate_pipeline<'input>(
    input: Option<TimebucketAggregate<'input>>,
    operation: AggregatePipelineElement,
) -> Option<TimebucketAggregate<'input>> {
    input.map(|aggregate| {
        match operation.inner {
            PipelineElement::Sample{width} => do_sample(aggregate, width),
            PipelineElement::Locf => do_locf(aggregate),
            PipelineElement::Interpolate{..} => do_linear_interpolate(aggregate),
        }
    })
}

#[pg_extern]
fn start_aggregate_pipeline(
    first: AggregatePipelineElement,
    second: AggregatePipelineElement,
) -> AggregatePipeline {
    AggregatePipeline{ pipeline: vec![first.inner, second.inner] }
}

#[pg_extern]
fn add_to_aggregate_pipeline(
    mut pipeline: AggregatePipeline,
    operation: AggregatePipelineElement
) -> AggregatePipeline {
    pipeline.pipeline.push(operation.inner);
    pipeline
}

fn do_sample<'input>(input: TimebucketAggregate<'input>, width: i64)
-> TimebucketAggregate<'input> {
    let mut builder = TimebucketAggregateBuilder::with_capacity(input.start.times.len(), *input.start.typ);
    let mut last_time = None;
    for (time, value) in input {
        let sample_time = match last_time {
            None => time,
            Some(mut last_time) => {
                if time < last_time + width {
                    continue
                }
                while time > last_time + width * 2 {
                    last_time += width;
                    builder.push(last_time, None);
                }
                last_time+width
            }
        };

        last_time = Some(sample_time);
        builder.push(sample_time, value);
    }
    builder.build()
}

fn do_locf<'input>(input: TimebucketAggregate<'input>) -> TimebucketAggregate<'input> {
    let mut builder = TimebucketAggregateBuilder::with_capacity(input.start.times.len(), *input.start.typ);
    let mut last = None;
    for (time, mut value) in input {
        match value {
            None => value = last,
            Some(..) => last = value,
        }
        builder.push(time, value);
    }
    builder.build()
}

fn do_linear_interpolate<'input>(input: TimebucketAggregate<'input>)
-> TimebucketAggregate<'input> {
    //FIXME typecheck
    let mut builder = TimebucketAggregateBuilder::with_capacity(input.start.times.len(), *input.start.typ);
    let mut last_time;
    let mut last_value;
    let mut iter = input.into_iter();
    loop {
        match iter.next() {
            None => return builder.build(),
            Some((time, None)) => builder.push(time, None),
            Some((time, Some(value))) => {
                builder.push(time, Some(value));
                last_time = time;
                last_value = value;
                break
            }
        }
    }

    let mut num_missing = 0;
    let mut last_missing = 0;
    loop {
        match iter.next() {
            Some((time, None)) => {
                last_missing = time;
                num_missing += 1;
                continue
            }
            Some((time, Some(value))) => {
                if num_missing > 0 {
                    let time_inc = (time - last_time) / (num_missing + 1) as TimestampTz;
                    let value_inc = (value - last_value) / (num_missing + 1);
                    for _ in 0..num_missing {
                        last_value += value_inc;
                        last_time += time_inc;
                        builder.push(last_time, Some(last_value));
                    }
                    num_missing = 0;
                }
                last_time = time;
                last_value = value;
                builder.push(time, Some(value));
            }
            None => {
                if num_missing > 0 {
                    let time_inc = (last_missing - last_time) / num_missing as TimestampTz;
                    for _ in 0..num_missing {
                        last_time += time_inc;
                        builder.push(last_time, None);
                    }
                }
                return builder.build()
            },
        }
    }
}

#[derive(PostgresType, Clone, Debug, Serialize, Deserialize)]
pub struct AggregatePipeline {
    pipeline: Vec<PipelineElement>,
}

#[derive(PostgresType, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct AggregatePipelineElement {
    inner: PipelineElement,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum PipelineElement {
    Sample{width: i64},
    Locf,
    Interpolate{min: Option<f64>, max: Option<f64>},
}


#[pg_extern]
fn unnest(
    input: Option<TimebucketAggregate<'static>>,
    typ: Option<AnyElement>,
) -> impl Iterator<Item=(name!(index, TimestampTz), name!(value, Option<AnyElement>))> + 'static {
    let typ = input.as_ref().map(|i| *i.start.typ).unwrap_or(0);
    // if let Some(input) = input {
    //     if input.typ != typ {
    //         panic!("invalid type for aggregate")
    //     }
    // }
    input.into_iter()
        .flat_map(|i| i.into_iter())
        .map(move |(time, value)| unsafe {
            (time, AnyElement::from_datum(value.unwrap_or(0), value.is_none(), typ))
        })
}



#[derive(PostgresType, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct AggregatePipelineExpand {
    inner: PipelineExpand,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum PipelineExpand {
    Unnest(pg_sys::Oid),
}

#[derive(PostgresType, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct AggregatePipelineSink {
    inner: PipelineSink,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum PipelineSink {
    Avg,
}

pub struct TimebucketAggregateBuilder {
    values: Vec<(TimestampTz, Option<pg_sys::Datum>)>,
    sorted: bool,
    typ: pg_sys::Oid,
}

impl TimebucketAggregateBuilder {
    pub fn new(typ: pg_sys::Oid) -> Self {
        Self {
            values: vec![],
            sorted: true,
            typ,
        }
    }

    pub fn with_capacity(cap: usize, typ: pg_sys::Oid) -> Self {
        Self {
            values: Vec::with_capacity(cap),
            sorted: true,
            typ,
        }
    }

    pub fn push(&mut self, time: TimestampTz, value: Option<Datum>) {
        self.sorted = self.values.last().map_or(true, |last| last.0 <= time);
        self.values.push((time, value));
    }

    pub fn sort(&mut self) {
        if self.sorted {
            return
        }

        self.values.sort_by_key(|v| v.0)
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn build(&mut self) -> TimebucketAggregate<'static> {
        let io = DatumIo::new(self.typ);

        let num_nulls = div_ceil(self.values.len(), 64);
        let num_offsets = if self.values.len() % 2 == 0 {
                self.values.len()
            } else {
                self.values.len() + 1
            } as usize;

        let mut times = Vec::with_capacity(self.values.len());
        let mut nulls = vec![0; num_nulls];
        let mut value_offsets = Vec::with_capacity(num_offsets);
        let mut values = Vec::new();

        let mut offset = 0;
        for (i, (time, value)) in self.values.iter().enumerate() {
            set_nulls_bit(&mut nulls, i, value.is_none());
            times.push(*time);
            value_offsets.push(offset);
            match value {
                None => continue,
                Some(value) =>
                    offset += io.write(*value, &mut values) as u32,
            }
        }

        // TODO with_capacity
        let mut bytes = Vec::new();
        tba_start::Ref {
            header: &0,
            typ: &self.typ,
            times: &*times,
            nulls: &*nulls,
            value_offsets: &*value_offsets,
        }.fill_vec(&mut bytes);

        bytes.append(&mut values);

        unsafe {
            set_varsize(bytes.as_mut_ptr() as *mut _, bytes.len() as i32);
        }

        let bytes = Box::leak(Vec::into_boxed_slice(bytes));
        let (start, values) = unsafe { tba_start::try_ref(bytes).unwrap() };

        TimebucketAggregate {
            start,
            values,
        }
    }
}


flat_serialize_macro::flat_serialize!{
    struct tba_start {
        header: u32,
        typ: pg_sys::Oid,
        len: u32,
        times: [TimestampTz; self.len],
        nulls: [u64; div_ceil(self.len, 64)], //TODO
        value_offsets: [u32; self.len + self.len % 2],
        // values: [u8], needs another step for now
    }
}

#[derive(PostgresType, Copy, Clone)]
#[inoutfuncs]
pub struct TimebucketAggregate<'input>{
    start: tba_start::Ref<'input>,
    values: &'input [u8],
}

pub type NullsMap<'i> = &'i [NullsBlock];
pub type NullsBlock = u64;

impl<'input> IntoIterator for TimebucketAggregate<'input> {
    type Item = (TimestampTz, Option<Datum>);

    type IntoIter = AggregateIter<'input>;

    fn into_iter(self) -> Self::IntoIter {
        let mut typlen = -1;
        let mut typbyval = false;
        let mut typalign = 0;
        unsafe {
            pg_sys::get_typlenbyvalalign(
                *self.start.typ,
                &mut typlen,
                &mut typbyval,
                &mut typalign,
            );
        }
        AggregateIter{
            agg: self,
            idx: 0,
            val_len: typlen,
            byval: typbyval,
        }
    }
}

pub struct AggregateIter<'input> {
    agg: TimebucketAggregate<'input>,
    idx: u32,
    val_len: i16,
    byval: bool,
}

impl<'input> Iterator for AggregateIter<'input> {
    type Item = (TimestampTz, Option<Datum>);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.idx as usize;
        if idx >= self.agg.start.times.len() {
            return None
        }
        let time = self.agg.start.times[idx];
        let is_null = get_nulls_bit(self.agg.start.nulls, idx);
        let value = if is_null {
                None
            } else {
                let offset = self.agg.start.value_offsets[idx] as usize;
                let mut value_bytes = [0; size_of::<Datum>()];
                let value_len =
                    if self.byval {
                        self.val_len as usize
                    } else {
                        size_of::<Datum>()
                    };
                let bytes = &self.agg.values[offset..offset+value_len];
                for i in 0..bytes.len() {
                    value_bytes[i] = bytes[i]
                }
                Some(Datum::from_ne_bytes(value_bytes))
            };
        self.idx += 1;
        return Some((time, value))
    }
}

impl<'input> InOutFuncs for TimebucketAggregate<'input> {
    fn output(&self, buffer: &mut StringInfo) {
        let mut first = true;
        for (time, value) in self.into_iter() {
            if !first {
                buffer.push_str(", ")
            }
            first = false;
            buffer.push_str("{");
            buffer.push_str(&time.to_string());
            match value {
                None => buffer.push_str(", NULL}"),
                Some(value) => {
                    buffer.push_str(", ");
                    buffer.push_str(&value.to_string());
                    buffer.push_str("}");
                },
            }
        }
    }

    fn input(input: &std::ffi::CStr) -> Self
    where
        Self: Sized {
        todo!()
    }
}


impl<'input> FromDatum for TimebucketAggregate<'input> {
    unsafe fn from_datum(datum: Datum, is_null: bool, _: pg_sys::Oid) -> Option<Self>
    where
        Self: Sized {
        if is_null {
            return None
        }

        let ptr = pg_sys::pg_detoast_datum_packed(datum as *mut pg_sys::varlena);
        let data_len = varsize_any(ptr);
        let bytes = slice::from_raw_parts(ptr as *mut u8, data_len);

        let (start, values) = match tba_start::try_ref(bytes) {
            Ok(wrapped) => wrapped,
            Err(e) => error!("invalid TimebucketAggregate {:?}", e),
        };

        TimebucketAggregate {
            start,
            values,
        }.into()
    }
}

impl<'input> IntoDatum for TimebucketAggregate<'input> {
    fn into_datum(self) -> Option<Datum> {
        Some(self.start.header as *const u32 as Datum)
    }

    fn type_oid() -> pg_sys::Oid {
        rust_regtypein::<Self>()
    }
}

pub struct DatumIo {
    typlen: i16,
    typbyval: bool,
    typalign: i8,
}

impl DatumIo {
    pub fn new(oid: pg_sys::Oid) -> Self {
        let mut typlen = 0;
        let mut typbyval = false;
        let mut typalign = 0;
        unsafe {
            pg_sys::get_typlenbyvalalign(
                oid,
                &mut typlen,
                &mut typbyval,
                &mut typalign,
            );
        }
        DatumIo {
            typlen,
            typbyval,
            typalign,
        }
    }

    pub fn may_be_toasted(&self) -> bool {
        self.typlen == -1
    }

    pub fn needed_alignment(&self) -> usize {
        match self.typalign as u8 {
            b'c' => 1,
            b's' => 2,
            b'i' => 4,
            b'd' => 8,
            _ => unreachable!(),
        }
    }

    pub fn get_bytes_size(&self, val: Datum) -> usize {
        //TODO packable
        let needed_alignment = self.needed_alignment();

        // let len = pg_sys::att_align_datum(0, self.typalign, self.typlen, val);
        // att_addlength_datum
        let mut len = if self.typlen > 0 {
            //TODO align
            self.typlen as usize
        } else if self.typlen == -1 {
            unsafe { varsize_any(val as *const _) }
        } else {
            assert!(self.typlen == -2);
            unsafe { pg_sys::strlen(val as *const _) as usize + 1 }
        };
        if len % needed_alignment != 0 {
            len += len % needed_alignment
        }
        len
    }

    pub fn write<'b>(&self, val: Datum, buffer: &mut Vec<u8>) -> usize {
        unsafe {
            let mut len;
            let byte_arr;
            let bytes;
            if self.typbyval {
                // store_att_byval
                len = self.typlen as usize;
                byte_arr = val.to_ne_bytes();
                bytes = &byte_arr[..len];
            } else if self.typlen == -1 {
                len = varsize(val as *mut _);
                bytes = slice::from_raw_parts(val as *mut _, len);
            } else if self.typlen == -2 {
                len = pg_sys::strlen(val as *const _) as usize + 1;
                bytes = slice::from_raw_parts(val as *mut _, len);
            } else {
                len = self.typlen as usize;
                bytes = slice::from_raw_parts(val as *mut _, len);
            }

            buffer.extend_from_slice(bytes);
            if len % self.needed_alignment() != 0 {
                let needed = len % self.needed_alignment();
                buffer.extend((0..needed).map(|_| 0));
                len += len % self.needed_alignment()
            }
            len
        }
    }
}

pub fn set_nulls_bit(nulls: &mut [NullsBlock], bit_num: usize, bit_value: bool) {
    let (idx, shift) = (bit_num/64, bit_num%64);
    let bit = 1 << shift;
    if bit_value {
        nulls[idx] |= bit
    } else {
        nulls[idx] &= !bit;
    }
}

pub fn get_nulls_bit(nulls: &[NullsBlock], bit_num: usize) -> bool {
    let (idx, shift) = (bit_num/64, bit_num%64);
    let mask = 1 << shift;
    (nulls[idx] & mask) != 0
}

// #[pg_extern]
// fn time_bucket_sample(interval: Interval);

pub struct Internal<T>(pub PgBox<T>);

impl<T> FromDatum for Internal<T> {
    #[inline]
    unsafe fn from_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _: pg_sys::Oid,
    ) -> Option<Internal<T>> {
        if is_null {
            None
        } else if datum == 0 {
            panic!("Internal-type Datum flagged not null but its datum is zero")
        } else {
            Some(Internal::<T>(PgBox::<T>::from_pg(datum as *mut T)))
        }
    }
}

impl<T> IntoDatum for Internal<T> {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        self.0.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        pg_sys::INTERNALOID
    }
}

fn div_ceil(a: usize, b: usize) -> usize {
    let (q, r) = (a / b, a % b);
    if r != 0 {
        q + 1
    } else {
        q
    }
}

#[pg_extern]
fn hello_ts_aggregates() -> &'static str {
    "Hello, ts_aggregates"
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_ts_aggregates() {
        assert_eq!("Hello, ts_aggregates", crate::hello_ts_aggregates());
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
