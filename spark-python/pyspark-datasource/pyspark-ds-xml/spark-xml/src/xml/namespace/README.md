# ignoreNamespace

If true, namespaces prefixes on XML elements and attributes are ignored.

Tags `<abc:author>` and `<def:author>` would, for example, be treated as if both are just `<author>`.

`Note that, at the moment, namespaces cannot be ignored on the rowTag element, only its children`.

Note that XML parsing is in general not namespace-aware even if false.
Defaults to false.
