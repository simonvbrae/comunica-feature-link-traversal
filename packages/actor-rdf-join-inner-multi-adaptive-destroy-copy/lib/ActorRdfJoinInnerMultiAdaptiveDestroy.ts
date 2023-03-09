import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  MediatorRdfJoin,
  IActorRdfJoinOutputInner,
} from "@comunica/bus-rdf-join";
import { ActorRdfJoin } from "@comunica/bus-rdf-join";
import { KeysRdfJoinEntriesSort, KeysRdfJoin } from '@comunica/context-entries-link-traversal';
import type { IMediatorTypeJoinCoefficients } from "@comunica/mediatortype-join-coefficients";
import type {
  IQueryOperationResultBindings,
  MetadataBindings,
  IJoinEntry,
} from "@comunica/types";
import { exit } from "process";
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
    // console.log("HEY: run adaptivejoin");
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
    let subContextWithoutCallback = action.context.set(KeysRdfJoin.skipAdaptiveJoin, true);
    let subContextWithoutCallback2 = subContextWithoutCallback.set(KeysRdfJoinEntriesSort.sortByCardinality, true);
    subContextWithoutCallback2 = subContextWithoutCallback2.set(KeysRdfJoinEntriesSort.sortZeroKnowledge, false);
    let context = subContextWithoutCallback2.set(KeysRdfJoin.adaptiveJoinCallback, () => bindingsStream.swapCallback());
    
    let entriesHaveIndexCardinalities : boolean = true;
    for (let entry of action.entries) {
      let x = await entry.output.metadata();
      let type : any = x.cardinality?.type;
      if (type !== "index") {
        entriesHaveIndexCardinalities = false;
      }
    }
    if (entriesHaveIndexCardinalities) {
      // If each entry has cardinalities from the index, disable restarting the query
      console.log("Going directly to phase two");
      context = subContextWithoutCallback2;
    }

    // Execute the join with the metadata we have now
    const firstOutput = await this.mediatorJoin.mediate({
      type: action.type,
      entries: this.cloneEntries(action.entries),
      context: context,
    });

    let bindingsStream = new BindingsStreamAdaptiveDestroy(
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
            context: subContextWithoutCallback2,
          })
        ).bindingsStream;
      },
      { timeout: this.timeout, autoStart: false }
    );

    return {
      result: {
        type: "bindings",
        bindingsStream: bindingsStream,
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
