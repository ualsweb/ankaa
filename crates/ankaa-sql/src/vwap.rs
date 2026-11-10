//! `vwap(price, size)`: the volume-weighted average price, `Σ price·size / Σ size`.
//!
//! Prices and sizes are fixed-point integers, so both sums are exact (`i128`) and one division
//! is made per group at the end, instead of two floating-point divisions per row as in
//! `sum((price / 1e8) * (size / 1e8)) / sum(size / 1e8)`. The result is in the unit of the
//! price column (ticks); `NULL` for a group whose sizes sum to zero.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int64Type};
use datafusion::common::{Result, ScalarValue, internal_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};

/// Precision of the two sums kept as state (they travel between partial and final aggregation).
const STATE: DataType = DataType::Decimal128(38, 0);

/// The `vwap` aggregate function.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Vwap {
    signature: Signature,
}

impl Default for Vwap {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for Vwap {
    fn name(&self) -> &'static str {
        "vwap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(One::default()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(format!("{}[notional]", args.name), STATE, true)),
            Arc::new(Field::new(format!("{}[volume]", args.name), STATE, true)),
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(Groups::default()))
    }
}

#[allow(clippy::cast_precision_loss)] // the result is an average price: f64 is its type
fn ratio(notional: i128, volume: i128) -> Option<f64> {
    (volume != 0).then(|| notional as f64 / volume as f64)
}

fn inputs(values: &[ArrayRef]) -> Result<(&arrow::array::Int64Array, &arrow::array::Int64Array)> {
    let [price, size] = values else {
        return internal_err!("vwap takes (price, size)");
    };
    Ok((
        price.as_primitive::<Int64Type>(),
        size.as_primitive::<Int64Type>(),
    ))
}

fn sums(states: &[ArrayRef]) -> Result<(&Decimal128Array, &Decimal128Array)> {
    let [notional, volume] = states else {
        return internal_err!("vwap state is (notional, volume)");
    };
    Ok((
        notional.as_primitive::<Decimal128Type>(),
        volume.as_primitive::<Decimal128Type>(),
    ))
}

fn decimal(values: Vec<i128>) -> Result<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::from(values).with_precision_and_scale(38, 0)?,
    ))
}

/// One group (aggregation without `GROUP BY`).
#[derive(Debug, Default)]
struct One {
    notional: i128,
    volume: i128,
}

impl Accumulator for One {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let (price, size) = inputs(values)?;
        for i in 0..price.len() {
            if price.is_valid(i) && size.is_valid(i) {
                let (p, s) = (i128::from(price.value(i)), i128::from(size.value(i)));
                self.notional += p * s;
                self.volume += s;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(ratio(self.notional, self.volume)))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Decimal128(Some(self.notional), 38, 0),
            ScalarValue::Decimal128(Some(self.volume), 38, 0),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let (notional, volume) = sums(states)?;
        for i in 0..notional.len() {
            if notional.is_valid(i) && volume.is_valid(i) {
                self.notional += notional.value(i);
                self.volume += volume.value(i);
            }
        }
        Ok(())
    }
}

/// Many groups.
#[derive(Debug, Default)]
struct Groups {
    notional: Vec<i128>,
    volume: Vec<i128>,
}

impl GroupsAccumulator for Groups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let (price, size) = inputs(values)?;
        self.notional.resize(total_num_groups, 0);
        self.volume.resize(total_num_groups, 0);
        let plain = price.null_count() == 0 && size.null_count() == 0 && opt_filter.is_none();
        let (prices, sizes) = (price.values(), size.values());
        for (i, &g) in group_indices.iter().enumerate() {
            let take = plain
                || (price.is_valid(i)
                    && size.is_valid(i)
                    && opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i)));
            if take {
                let (p, s) = (i128::from(prices[i]), i128::from(sizes[i]));
                self.notional[g] += p * s;
                self.volume[g] += s;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let notional = emit_to.take_needed(&mut self.notional);
        let volume = emit_to.take_needed(&mut self.volume);
        Ok(Arc::new(Float64Array::from_iter(
            notional.iter().zip(&volume).map(|(&n, &v)| ratio(n, v)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![
            decimal(emit_to.take_needed(&mut self.notional))?,
            decimal(emit_to.take_needed(&mut self.volume))?,
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let (notional, volume) = sums(values)?;
        self.notional.resize(total_num_groups, 0);
        self.volume.resize(total_num_groups, 0);
        for (i, &g) in group_indices.iter().enumerate() {
            if notional.is_valid(i) && volume.is_valid(i) {
                self.notional[g] += notional.value(i);
                self.volume[g] += volume.value(i);
            }
        }
        Ok(())
    }

    /// Each row as the state of a group of its own (the aggregate uses it when grouping does
    /// not reduce the rows: it skips the partial step).
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let (price, size) = inputs(values)?;
        let (mut notional, mut volume) = (
            Vec::with_capacity(price.len()),
            Vec::with_capacity(price.len()),
        );
        for i in 0..price.len() {
            let take = price.is_valid(i)
                && size.is_valid(i)
                && opt_filter.is_none_or(|f| f.is_valid(i) && f.value(i));
            let (p, s) = if take {
                (i128::from(price.value(i)), i128::from(size.value(i)))
            } else {
                (0, 0)
            };
            notional.push(p * s);
            volume.push(s);
        }
        Ok(vec![decimal(notional)?, decimal(volume)?])
    }

    fn size(&self) -> usize {
        (self.notional.capacity() + self.volume.capacity()) * size_of::<i128>()
    }
}
