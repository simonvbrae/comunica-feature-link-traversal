import { ActionContextKey } from '@comunica/core';
import type { AnnotateSourcesType } from '@comunica/types-link-traversal';
import type { Bindings,
  IPhysicalQueryPlanLogger,
  QueryExplainMode,
  IProxyHandler,
  ICliArgsHandler,
  DataSources,
  IDataSource,
  IDataDestination,
  MetadataBindings } from '@comunica/types';

/**
 * When adding entries to this file, also add a shortcut for them in the contextKeyShortcuts TSDoc comment in
 * ActorIniQueryBase in @comunica/actor-init-query if it makes sense to use this entry externally.
 * Also, add this shortcut to IQueryContextCommon in @comunica/types.
 */

 export const KeysRdfResolveHypermediaLinks = {
  /**
   * A flag for indicating if traversal should be enabled. Defaults to true.
   */
  traverse: new ActionContextKey<boolean>('@comunica/actor-rdf-resolve-hypermedia-links-traverse:traverse'),
  /**
   * Context entry for indicating the type of source annotation.
   */
  annotateSources: new ActionContextKey<AnnotateSourcesType>(
    '@comunica/bus-rdf-resolve-hypermedia-links:annotateSources',
  ),
};
export const KeysRdfJoin = {
  /**
   * If adaptive joining must not be done.
   */
  skipAdaptiveJoin: new ActionContextKey<IDataDestination>('@comunica/bus-rdf-join:skipAdaptiveJoin'),
  /**
   * Callback which starts phase two of adaptive join.
   */
  adaptiveJoinCallback: new ActionContextKey<any>('@comunica/bus-rdf-join:adaptiveJoinCallback')
};
export const KeysRdfJoinEntriesSort = {
  /**
   * A flag for whether to use the cardinality sort.
   */
  sortByCardinality: new ActionContextKey<boolean>('@comunica/bus-rdf-join-entries-sort:sortByCardinality'),
  /**
   * A flag for whether to use the zero knowledge sort.
   */
  sortZeroKnowledge: new ActionContextKey<boolean>('@comunica/bus-rdf-join-entries-sort:sortZeroKnowledge')
};