import {
  type ActorInitQueryBase,
  QueryEngineBase,
} from "@comunica/actor-init-query";
import { type MediatorDereferenceRdf } from "@comunica/bus-dereference-rdf";
import {
  type IActionRdfMetadataExtract,
  type IActorRdfMetadataExtractOutput,
  type IActorRdfMetadataExtractArgs,
  ActorRdfMetadataExtract,
} from "@comunica/bus-rdf-metadata-extract";
import { KeysQueryOperation, KeysInitQuery } from "@comunica/context-entries";
import { type IActorTest } from "@comunica/core";
import { type IActionContext, type IQueryEngine } from "@comunica/types";
import type * as RDF from "@rdfjs/types";
import { exit } from "process";
import { storeStream } from "rdf-store-stream";

/**
 * An RDF Metadata Extract Actor that extracts dataset metadata from their VOID descriptions
 */
export class ActorRdfMetadataExtractVoidDescription
  extends ActorRdfMetadataExtract
  implements IActorRdfMetadataExtractVoidDescriptionArgs
{
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly actorInitQuery: ActorInitQueryBase;
  public readonly voidDatasetDescriptionPredicates: string[];

  private readonly voidDatasetDescriptionPredicatesSet: Set<string>;
  private readonly queryEngine: IQueryEngine;

  private static readonly predicateCardinalitiesByDataset: Map<
    string,
    Map<string, number>
  > = new Map<string, Map<string, number>>();

  public constructor(args: IActorRdfMetadataExtractVoidDescriptionArgs) {
    super(args);
    console.log("ActorRdfMetadataExtractVoidDescription: constructor");
    this.actorInitQuery = args.actorInitQuery;
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.voidDatasetDescriptionPredicates =
      args.voidDatasetDescriptionPredicates;
    this.voidDatasetDescriptionPredicatesSet = new Set<string>(
      args.voidDatasetDescriptionPredicates
    );
    this.queryEngine = new QueryEngineBase(args.actorInitQuery);
  }

  public async test(action: IActionRdfMetadataExtract): Promise<IActorTest> {
    console.log("ActorRdfMetadataExtractVoidDescription: test");
    if (!action.context.get(KeysInitQuery.query)) {
      throw new Error(
        `Actor ${this.name} can only work in the context of a query.`
      );
    }
    if (!action.context.get(KeysQueryOperation.operation)) {
      throw new Error(
        `Actor ${this.name} can only work in the context of a query operation.`
      );
    }
    return true;
  }

  public async run(
    action: IActionRdfMetadataExtract
  ): Promise<IActorRdfMetadataExtractOutput> {
    console.log("ActorRdfMetadataExtractVoidDescription: run", action.url);
    // console.log("kk");
    // console.trace();
    // exit(1);
    const quad: RDF.Quad = action.context.getSafe(
      KeysQueryOperation.operation
    ) as RDF.Quad;
    if (!this.checkIfMetadataExistsForUrl(action.url)) {
      const voidMetadataDescriptions: string[] =
        await this.extractVoidDatasetDescriptionLinks(action.metadata);
      if (voidMetadataDescriptions.length > 0) {
        await Promise.all(
          voidMetadataDescriptions.map((url) =>
            this.dereferenceVoidDatasetDescription(url, action.context)
          )
        );
      }
    }
    return this.extractMetadataForPredicate(action.url, quad.predicate.value);
  }

  private async dereferenceVoidDatasetDescription(
    url: string,
    context: IActionContext
  ): Promise<void> {
    console.log("Calling dereference function, url: " + url);
    const response = await this.mediatorDereferenceRdf.mediate({
      url: url,
      context: context,
    });

    const store = await storeStream<RDF.Quad>(response.data);

    const query = `
      PREFIX void: <http://rdfs.org/ns/void#>

      SELECT ?dataset ?property ?propertyCardinality WHERE {
        ?dataset a void:Dataset ;
          void:propertyPartition [
            ?property ?propertyCardinality
          ] .
      }
    `;

    const bindingsStream = await this.queryEngine.queryBindings(query, {
      sources: [store],
      lenient: false,
    });
    const bindingsArray: RDF.Bindings[] = await bindingsStream.toArray();
    // console.log("bindingsArray");
    // console.log(bindingsArray);

    for (const bindings of bindingsArray) {
      const dataset = bindings.get("dataset");
      const property = bindings.get("property");
      const propertyCardinality = bindings.get("propertyCardinality");
      if (dataset && property && propertyCardinality) {
        if (
          !ActorRdfMetadataExtractVoidDescription.predicateCardinalitiesByDataset.has(
            dataset.value
          )
        ) {
          ActorRdfMetadataExtractVoidDescription.predicateCardinalitiesByDataset.set(
            dataset.value,
            new Map<string, number>()
          );
        }
        const datasetData =
          ActorRdfMetadataExtractVoidDescription.predicateCardinalitiesByDataset.get(
            dataset.value
          ) as Map<string, number>;
        // logging it like this below
        // eslint-disable-next-line no-console

        datasetData.set(
          property.value,
          (datasetData.get(property.value) ?? 0) +
            parseInt(propertyCardinality.value)
        );

        // console.log(
        //   "Cardinality",
        //   dataset.value,
        //   property.value,
        //   datasetData.get(property.value)
        // );
      }
    }
  }

  private extractVoidDatasetDescriptionLinks(
    metadata: RDF.Stream
  ): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
      console.log("calling extractVoidDatasetDescriptionLinks");
      const datasetDescriptionLinks: Set<string> = new Set<string>();
      metadata
        .on("data", (quad: RDF.Quad) => {
          if (
            this.voidDatasetDescriptionPredicatesSet.has(quad.predicate.value)
          ) {
            datasetDescriptionLinks.add(quad.object.value);
          }
        })
        .on("error", reject)
        .on("end", () => resolve([...datasetDescriptionLinks.values()]));
    });
  }

  private checkIfMetadataExistsForUrl(url: string): boolean {
    for (const key of ActorRdfMetadataExtractVoidDescription.predicateCardinalitiesByDataset.keys()) {
      if (url.startsWith(key)) {
        return true;
      }
    }
    return false;
  }

  private extractMetadataForPredicate(
    url: string,
    predicate: string
  ): IActorRdfMetadataExtractOutput {
    const cardinality: Record<string, string | number> = {
      type: "estimate",
      value: Number.POSITIVE_INFINITY,
    };
    console.log("calling extractMetadataForPredicate");
    for (const [
      key,
      data,
    ] of ActorRdfMetadataExtractVoidDescription.predicateCardinalitiesByDataset) {
      if (url.startsWith(key) && data.has(predicate)) {
        cardinality.dataset = key;
        cardinality.value = data.get(predicate) as number;
      }
    }
    console.log(cardinality);
    return { metadata: { cardinality: cardinality } };
  }
}

export interface IActorRdfMetadataExtractVoidDescriptionArgs
  extends IActorRdfMetadataExtractArgs {
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase;
  /**
   * The Dereference RDF mediator.
   * @default {<urn:comunica:default:dereference-rdf/mediators#main>}
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The predicates to follow in search of VOID dataset secriptions.
   * @default {http://www.w3.org/ns/solid/terms#voidDescription}
   */
  voidDatasetDescriptionPredicates: string[];
}
