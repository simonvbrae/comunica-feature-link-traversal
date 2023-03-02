import type { Bindings, BindingsStream } from '@comunica/types';
import type { TransformIteratorOptions } from 'asynciterator';
import { TransformIterator } from 'asynciterator';
import { exit } from 'process';

/**
 * An iterator that starts by iterating over the first iterator,
 * and switches to a second iterator after a timeout.
 *
 * If the first iterator ends before the timout, the second iterator will not be started.
 *
 * This will ensure that results are not duplicated after switching.
 */
export class BindingsStreamAdaptiveDestroy extends TransformIterator<Bindings> {
  private readonly delayedSource: () => Promise<BindingsStream>;
  private readonly pushedBindings: Map<string, number>;

  public swapCallback: any;
  private inPhaseTwo: boolean;

  public constructor(
    source: BindingsStream,
    delayedSource: () => Promise<BindingsStream>,
    options: TransformIteratorOptions<Bindings> & { timeout: number },
  ) {
    super(source, options);
    this.delayedSource = delayedSource;
    this.pushedBindings = new Map();
    this.inPhaseTwo = false;

    this.swapCallback = () => {
      console.log("cb: Called swapCallback");
      console.log("this.source, this.source?.done, this.inPhaseTwo");
      console.log(this.source !== undefined, this.source?.done, this.inPhaseTwo);
      if (this.source && !this.source.done && !this.inPhaseTwo) {
        // Stop current iterator
        this.source.destroy();

        // Start a new iterator
        this._source = undefined;
        this.inPhaseTwo = true;
        this._createSource = this.delayedSource;
        this._loadSourceAsync();
      }
    };
  }

  public setSource(source : any) {
    super.setProperties({
      source: source
    });
  }

  protected _init(autoStart: boolean): void {
    super._init(autoStart);
  }

  protected _push(item: Bindings): void {
    // eslint-disable-next-line @typescript-eslint/no-base-to-string
    const bindingsKey = item.toString();
    if (!this.inPhaseTwo) {
      // If we're in the first stream, store the pushed bindings
      this.pushedBindings.set(bindingsKey, (this.pushedBindings.get(bindingsKey) || 0) + 1);
      super._push(item);
    } else {
      // If we're in the second stream, only push the bindings that were not yet pushed in the first stream
      const pushedBefore = this.pushedBindings.get(bindingsKey);
      if (pushedBefore) {
        this.pushedBindings.set(bindingsKey, pushedBefore - 1);
      } else {
        super._push(item);
      }
    }
  }

  protected _end(destroy: boolean): void {
    super._end(destroy);
  }
}
