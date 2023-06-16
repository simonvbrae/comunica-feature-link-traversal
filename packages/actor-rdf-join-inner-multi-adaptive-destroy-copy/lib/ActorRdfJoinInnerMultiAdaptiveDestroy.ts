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
  public readonly useCallback: boolean;
  public readonly useTimeout: boolean;

  public constructor(args: IActorRdfJoinInnerMultiAdaptiveDestroyArgs) {
    super(args, {
      logicalType: "inner",
      physicalName: "multi-adaptive-destroy",
    });
    this.timeout = args.timeout;
    this.skipAdaptiveJoin = args.skipAdaptiveJoin;
    this.useCallback = args.useCallback;
    this.useTimeout = args.useTimeout;
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
    subContextWithoutCallback = subContextWithoutCallback.set(KeysRdfJoin.test, 100);
    subContextWithoutCallback = subContextWithoutCallback.set(KeysRdfJoinEntriesSort.sortByCardinality, true);
    subContextWithoutCallback = subContextWithoutCallback.set(KeysRdfJoinEntriesSort.sortZeroKnowledge, false);
    
    let context = subContextWithoutCallback.set(KeysRdfJoin.adaptiveJoinCallback, () => {
      console.log("calling swapCallback in MultiAdaptive");
      bindingsStream.swapCallback();
    });
    context = context.set(KeysRdfJoin.test, 200);
    
    let allEntriesHaveIndexCardinalities : boolean = true;
    for (let entry of action.entries) {
      let x = await entry.output.metadata(); //TODO use entry.metadata.cardinality?
      let type : any = x.cardinality?.type;
      if (type !== "index") {
        allEntriesHaveIndexCardinalities = false;
      }
    }
    if (allEntriesHaveIndexCardinalities) {
      console.log("Going directly to phase two");
      context = subContextWithoutCallback;
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
        // Restart with the latest metadata
        console.log("");
        console.log("");
        console.log("---------------SWAP--------------");
        console.log("");
        console.log("");
        return (
          await this.mediatorJoin.mediate({
            type: action.type,
            entries: this.cloneEntries(action.entries),
            context: subContextWithoutCallback,
          })
        ).bindingsStream;
      },
      { timeout: this.timeout, useCallback: this.useCallback, useTimeout: this.useTimeout, autoStart: false }
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
  useTimeout: boolean;
  useCallback: boolean;
}
