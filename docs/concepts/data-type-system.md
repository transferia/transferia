---
title: "{{ data-transfer-name }} data type system"
description: "Learn about the data types used in {{ DC }} {{ data-transfer-name }}"
---

# {{ data-transfer-name }} data type system

The **Data type system** provides classes and conversion rules representing common data types. It allows for constructing in-memory representations of types, inspecting said representations, combining them to derive new types, and serializing and deserializing them.

## Built-in types

Type system provides built-in types similar to the most commonly used types. For convenience, the types are divided into the following groups:

* [Integers](#integers)
* [Floating point numbers](#floating-point-numbers)
* [Strings](#strings)
* [Time](#time)

### Integers

* `Boolean` is a boolean type with two possible values: `true` and `false`.
* `Int8` is a signed integer, `1` byte.
* `Int16` is a signed integer, `2` bytes.
* `Int32` is a signed integer, `4` bytes.
* `Int64` is a signed integer, `8` bytes.
* `Uint8` is an unsigned integer, `1` byte.
* `Uint16` is an unsigned integer, `2` bytes.
* `Uint32` is an unsigned integer, `4` bytes.
* `Uint64` is an unsigned integer, `8` bytes.

### Floating point numbers

Floating point numbers are number types that store values in the form of a mantissa and exponent, in accordance with [IEEE-754 ![external link](../_assets/external-link.svg)](https://en.wikipedia.org/wiki/IEEE_754).

* `float` is a floating point number, four bytes.
* `double` is a decimal number; for type fullness, {{ data-transfer-name }} stores it in a string format. It can cast it to the most precise floating point data type in most databases.

### Strings

Strings are types for working with text and binary data.

* `string` is an arbitrary binary blob.
* `utf8` is text encoded in [Utf8 ![external link](../_assets/external-link.svg)](https://www.w3schools.com/charsets/ref_html_utf8.asp).

### Time

Time types are used for storing time. This type represents an instant in time with nanosecond precision, but for some defined types, in can round down to:

* `date` - a day.
* `datetime` - a second.
* `timestamp` - the most precise time representation.

## Additional Data Types

{{ data-transfer-name }} employs an open-type data system.

Open-type systems declare an infinite type conversion by adding opaque data type. In a matter of simplicity, we define everything that's not as simple as the type above as ANY data type. The only requirement for this type is to be somehow serializable (usually JSON).

Each such type is open for improvement by adding extra type tags. This tag is usually invisible to the user and only used by the system itself (or as part of target data serialization). Most ANY types are, in fact, container data types.

## Container data types

Container types are parametric types that provide common data structures. We define several containers to simplify data integration.

* `optional <T>` - a container that may or may not contain a value of type `T`. Most databases have such a concept as nullable columns. Each nullable column transforms into this container type.

* `list <T>` - a list of values of type `T`. In general, a List is translated into a dynamic array. Most databases support this container.

* `dict <K, V>` - an associative container that maps to one key of type `K` exactly one value of type `V`.

If we face a source or target with limited complex type support, we downcast it to JSON built-in.

## See also

* [{#T}](../concepts/index.md)
