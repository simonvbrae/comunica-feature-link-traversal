# Comunica VOID Description RDF Metadata Extract Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-rdf-metadata-extract-void-description.svg)](https://www.npmjs.com/package/@comunica/actor-rdf-metadata-extract-void-description)

An [RDF Metadata Extract](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-metadata-extract) actor that attempts to discover [VOID dataset descriptions](https://www.w3.org/TR/void/) during query execution, and extracts the metadata they contain to provide better estimates of predicate cardinalities to the query engine.

This module is part of the [Comunica framework](https://github.com/comunica/comunica), and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-rdf-metadata-extract-void-description
```

## Metadata entries

This actor provides the following information in the metadata object.

* `cardinality`: The predicate cardinality of the current query operation, and `Infinity` when no VOID metadata has been discovered for the dataset and predicate
* `dataset`: The dataset IRI from the matching VOID dataset description, when the metadata for a dataset has been discovered

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-metadata-extract-void-description/^0.0.1/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-metadata-extract/actors#void-description",
      "@type": "ActorRdfMetadataExtractVoidDescription",
      "actorInitQuery": { "@id": "urn:comunica:default:init/actors#query" },
      "mediatorDereferenceRdf": { "@id": "urn:comunica:default:dereference-rdf/mediators#main" },
      "voidDatasetDescriptionPredicates": [ "http://www.w3.org/ns/solid/terms#voidDescription" ]
    }
  ]
}
```

### Config Parameters

* `actorInitQuery`: An instance of [ActorInitQuery](https://github.com/comunica/comunica/tree/master/packages/actor-init-query), defaults to `urn:comunica:default:init/actors#query`.
* `mediatorDereferenceRdf`: A mediator over the [Dereference RDF bus](https://github.com/comunica/comunica/tree/master/packages/bus-dereference-rdf).
* `voidDatasetDescriptionPredicates`: Set of predicates that can be assumed to link to a [VOID dataset description](https://www.w3.org/TR/void/).
