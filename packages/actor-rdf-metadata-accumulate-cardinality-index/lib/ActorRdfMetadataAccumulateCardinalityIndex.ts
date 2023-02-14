import type { IActionRdfMetadataAccumulate, IActorRdfMetadataAccumulateOutput,
  IActorRdfMetadataAccumulateArgs } from '@comunica/bus-rdf-metadata-accumulate';
import { ActorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import type { IActorTest } from '@comunica/core';
import type { QueryResultCardinality } from '@comunica/types';
import { exit } from 'process';

/**
 * A comunica Cardinality RDF Metadata Accumulate Actor.
 */
export class ActorRdfMetadataAccumulateCardinalityIndex extends ActorRdfMetadataAccumulate {
  public constructor(args: IActorRdfMetadataAccumulateArgs) {
    super(args);
  }

  public async test(action: IActionRdfMetadataAccumulate): Promise<IActorTest> {
    return true;
  }
  
  public async run(action: IActionRdfMetadataAccumulate): Promise<IActorRdfMetadataAccumulateOutput> {
    return { metadata: { }};
  }
}