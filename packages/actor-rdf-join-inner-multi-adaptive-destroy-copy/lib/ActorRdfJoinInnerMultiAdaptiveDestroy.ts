import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  MediatorRdfJoin,
  IActorRdfJoinOutputInner,
} from "@comunica/bus-rdf-join";
import { ActorRdfJoin } from "@comunica/bus-rdf-join";
import { KeysRdfJoin, KeysRdfJoinEntriesSort } from "@comunica/context-entries";
import type { IMediatorTypeJoinCoefficients } from "@comunica/mediatortype-join-coefficients";
import type {
  IQueryOperationResultBindings,
  MetadataBindings,
  IJoinEntry,
} from "@comunica/types";
import { BindingsStreamAdaptiveDestroy } from "./BindingsStreamAdaptiveDestroy";

/**
 * A comunica Inner Multi Adaptive Destroy RDF Join Actor.
 */
export class ActorRdfJoinInnerMultiAdaptiveDestroy extends ActorRdfJoin {
  public readonly mediatorJoin: MediatorRdfJoin;
  public readonly timeout: number;
  public readonly skipAdaptiveJoin: boolean;

  public constructor(args: IActorRdfJoinInnerMultiAdaptiveDestroyArgs) {
    super(args, {
      logicalType: "inner",
      physicalName: "multi-adaptive-destroy",
    });
    this.timeout = args.timeout;
    this.skipAdaptiveJoin = args.skipAdaptiveJoin;
  }

  public async test(
    action: IActionRdfJoin
  ): Promise<IMediatorTypeJoinCoefficients> {
    if (
      action.context.get(KeysRdfJoin.skipAdaptiveJoin) ||
      this.skipAdaptiveJoin
    ) {
      throw new Error(
        `Actor ${this.name} could not run because adaptive join processing is disabled.`
      );
    }
    return super.test(action);
  }

  public async run(
    action: IActionRdfJoin
  ): Promise<IQueryOperationResultBindings> {
    return super.run(action);
  }

  protected cloneEntries(entries: IJoinEntry[]): IJoinEntry[] {
    return entries.map((entry) => ({
      operation: entry.operation,
      output: {
        ...entry.output,
        // Clone stream, as we'll also need it later
        bindingsStream: entry.output.bindingsStream.clone(),
      },
    }));
  }

  protected async getOutput(
    action: IActionRdfJoin
  ): Promise<IActorRdfJoinOutputInner> {
    // Disable adaptive joins in recursive calls to this bus, to avoid infinite recursion on this actor.
    const subContext0 = action.context.set(KeysRdfJoin.skipAdaptiveJoin, true);
    // Configure sort actors
    const subContext1 = subContext0.set(
      KeysRdfJoinEntriesSort.sortZeroKnowledge,
      false
    );
    const subContext = subContext1.set(
      KeysRdfJoinEntriesSort.sortByCardinality,
      true
    );

    // Execute the join with the metadata we have now
    const firstOutput = await this.mediatorJoin.mediate({
      type: action.type,
      entries: this.cloneEntries(action.entries),
      context: subContext,
    });

    const subContext2 = subContext.set(
      KeysRdfJoinEntriesSort.sortZeroKnowledge,
      false
    );
    const subContext3 = subContext2.set(
      KeysRdfJoinEntriesSort.sortByCardinality,
      true
    );

    return {
      result: {
        type: "bindings",
        bindingsStream: new BindingsStreamAdaptiveDestroy(
          firstOutput.bindingsStream,
          async () => {
            // Restart the join with the latest metadata
            console.log("");
            console.log("");
            console.log("---------------SWAP--------------");
            console.log("");
            console.log("");
            return (
              await this.mediatorJoin.mediate({
                type: action.type,
                entries: this.cloneEntries(action.entries),
                context: subContext3,
              })
            ).bindingsStream;
          },
          { timeout: this.timeout, autoStart: false }
        ),
        metadata: firstOutput.metadata,
      },
    };
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[]
  ): Promise<IMediatorTypeJoinCoefficients> {
    // Dummy join coefficients to make sure we always run first
    return {
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    };
  }
}

export interface IActorRdfJoinInnerMultiAdaptiveDestroyArgs
  extends IActorRdfJoinArgs {
  mediatorJoin: MediatorRdfJoin;
  /**
   * @default {1000}
   */
  timeout: number;
  skipAdaptiveJoin: boolean;
}
