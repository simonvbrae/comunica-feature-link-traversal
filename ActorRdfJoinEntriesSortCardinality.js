"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ActorRdfJoinEntriesSortCardinality = void 0;
const bus_rdf_join_entries_sort_1 = require("@comunica/bus-rdf-join-entries-sort");
/**
 * An actor that sorts join entries by increasing cardinality.
 */
class ActorRdfJoinEntriesSortCardinality extends bus_rdf_join_entries_sort_1.ActorRdfJoinEntriesSort {
    constructor(args) {
        super(args);
    }
    async test(action) {
        return true;
    }
    async run(action) {
        const entries = [...action.entries]
            .sort((entryLeft, entryRight) => {
                if (entryLeft.metadata.cardinality.type === "index" && entryRight.metadata.cardinality.type === "exact"){
                    return -1;
                } else if (entryLeft.metadata.cardinality.type === "exact" && entryRight.metadata.cardinality.type === "index"){
                    return 1;
                } else {
                    return entryLeft.metadata.cardinality.value - entryRight.metadata.cardinality.value;
                }});
        return { entries };
    }
}
exports.ActorRdfJoinEntriesSortCardinality = ActorRdfJoinEntriesSortCardinality;
//# sourceMappingURL=ActorRdfJoinEntriesSortCardinality.js.map