import { KeysRdfResolveQuadPattern } from '@comunica/context-entries';
import { Bus, ActionContext } from '@comunica/core';
import { translate } from 'sparqlalgebrajs';
import {
  ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris,
} from '../lib/ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris';

describe('ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris instance', () => {
    let actor: ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris({
        name: 'actor',
        bus,
        extractSubjects: true,
        extractPredicates: true,
        extractObjects: true,
        extractGraphs: true,
        extractVocabIris: true,
      });
    });

    it('should test', () => {
      return expect(actor.test({ operation: <any> 'bla', context: new ActionContext({}) }))
        .resolves.toEqual(true);
    });

    it('should run on empty context', async() => {
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p> <ex:o> } }`, { quads: true });
      expect(await actor.run({ operation, context: new ActionContext({}) })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'ex:s', 'ex:p', 'ex:o', 'ex:g' ],
        }),
      });
    });

    it('should run on context with 2 sources', async() => {
      expect(await actor.run({
        operation: <any> 'bla',
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'a', 'b' ],
        }),
      })).toEqual({
        operation: <any> 'bla',
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'a', 'b' ],
        }),
      });
    });

    it('should run on context with 0 sources', async() => {
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p> <ex:o> } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'ex:s', 'ex:p', 'ex:o', 'ex:g' ],
        }),
      });
    });

    it('should run on context with 2 sources and operation', async() => {
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p> <ex:o> } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'a', 'b' ],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [ 'a', 'b' ],
        }),
      });
    });

    it('should run on context with 0 sources and operation', async() => {
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p> <ex:o> } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [
            'ex:s',
            'ex:p',
            'ex:o',
            'ex:g',
          ],
        }),
      });
    });

    it('should run on context with 0 sources and operation when only selecting some terms', async() => {
      actor = new ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris({
        name: 'actor',
        bus,
        extractSubjects: true,
        extractPredicates: false,
        extractObjects: true,
        extractGraphs: false,
        extractVocabIris: true,
      });
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p> <ex:o> } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [
            'ex:s',
            'ex:o',
          ],
        }),
      });
    });

    it('should run on context with 0 sources and operation with variables', async() => {
      const operation = translate(`SELECT * { GRAPH ?g { ?s ?p ?o } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      });
    });

    it('should run on context with 0 sources and operation with property paths', async() => {
      const operation = translate(`SELECT * { GRAPH <ex:g> { <ex:s> <ex:p>* <ex:o> } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [
            'ex:s',
            'ex:o',
            'ex:g',
          ],
        }),
      });
    });

    it('should run on context with 0 sources and operation with property paths with variables', async() => {
      const operation = translate(`SELECT * { GRAPH ?g { ?s <ex:p>* ?o } }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      });
    });

    it('should run when not extracting vocab IRIs', async() => {
      actor = new ActorOptimizeQueryOperationSetSeedSourcesQuadpatternIris({
        name: 'actor',
        bus,
        extractSubjects: true,
        extractPredicates: false,
        extractObjects: true,
        extractGraphs: false,
        extractVocabIris: false,
      });
      const operation = translate(`SELECT * { <ex:s> a <ex:TYPE>. <ex:s> <ex:p> <ex:o> }`, { quads: true });
      expect(await actor.run({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [],
        }),
      })).toEqual({
        operation,
        context: new ActionContext({
          [KeysRdfResolveQuadPattern.sources.name]: [
            'ex:s',
            'ex:o',
          ],
        }),
      });
    });
  });
});
