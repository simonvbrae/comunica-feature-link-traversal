import type { IActionRdfMetadataAccumulate, IActorRdfMetadataAccumulateOutput,
  IActorRdfMetadataAccumulateArgs } from '@comunica/bus-rdf-metadata-accumulate';
import { ActorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import type { IActorTest } from '@comunica/core';
import type { QueryResultCardinality } from '@comunica/types';

/**
 * A comunica Cardinality RDF Metadata Accumulate Actor.
 */
export class ActorRdfMetadataAccumulateCardinality extends ActorRdfMetadataAccumulate {
  public constructor(args: IActorRdfMetadataAccumulateArgs) {
    super(args);
  }

  public async test(action: IActionRdfMetadataAccumulate): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionRdfMetadataAccumulate): Promise<IActorRdfMetadataAccumulateOutput> {
    // Return default value on initialize
    if (action.mode === 'initialize') {
      return { metadata: { cardinality: { type: 'exact', value: 0 }}};
    }
    
    if (action.accumulatedMetadata.cardinality_index) {
      console.log("OK");
      console.log(action.accumulatedMetadata.cardinality_index);
    }
    if (action.appendingMetadata.cardinality_index) {
      console.log("OK2");
      console.log(action.appendingMetadata.cardinality_index);
    }

    // Otherwise, attempt to update existing value
    const cardinality: QueryResultCardinality = { ...action.accumulatedMetadata.cardinality };

    const accumulatedMetadataType : any = cardinality.type;
    if (accumulatedMetadataType === "index") {
        // If the cardinality comes from an index
        // keep the cardinality
        return { metadata: { cardinality }}
    } else if (action.appendingMetadata.cardinality) {
      const appendingMetadataType : any = action.appendingMetadata.cardinality.type
      if (appendingMetadataType === "index") {
        return { metadata: {cardinality: action.appendingMetadata.cardinality} }
      }
    }

    if (cardinality.dataset) {
      if (action.appendingMetadata.cardinality.dataset &&
        cardinality.dataset !== action.appendingMetadata.cardinality.dataset) {
        // If the accumulated cardinality is dataset-wide,
        // and the appending cardinality refers to another dataset,
        // remove the dataset scopes.
        delete cardinality.dataset;
      } else {
        // If the accumulated cardinality is dataset-wide,
        // and the appending cardinality refers to a dataset subset,
        // keep the accumulated cardinality unchanged.
        return { metadata: { cardinality }};
      }
    }

    if (!action.appendingMetadata.cardinality || !Number.isFinite(action.appendingMetadata.cardinality.value)) {
      // We're already at infinite, so ignore any later metadata
      cardinality.type = 'estimate';
      cardinality.value = Number.POSITIVE_INFINITY;
    } else {
      if (action.appendingMetadata.cardinality.type === 'estimate') {
        cardinality.type = 'estimate';
      }
      cardinality.value += action.appendingMetadata.cardinality.value;
    }

    return { metadata: { cardinality }};
  }
}
