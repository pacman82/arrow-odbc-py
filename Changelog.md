# Changelog

## 0.1.8

Fix: An upstream issue causing overflows for `timestamp['us']` has been fixed.

## 0.1.7

Fix: `BatchReader.__next__()` now returns `pyarrow.RecordBatch`. Previous version returned a `StructArray`.
Fix: `BatchReader.schema()` now return `pyarrow.Schema` instead of `pyarrow.Datatype`.

## 0.1.6

Replace maturin with milksnake

## 0.1.5

* Fix windows wheel

## 0.1.4

* Fixed a memory leak, than iterating over batches.

## 0.1.1-3

Test Release process for Wheels

## 0.1.0

Initial release
