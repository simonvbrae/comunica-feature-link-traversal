FROM node:16

# Install location
ENV dir /var/www/@comunica/query-sparql-link-traversal/

# Copy the engine files (generated from package.json!files)
COPY config ${dir}/config/
COPY lib/*.js ${dir}/lib/
COPY bin/*.js ${dir}/bin/
COPY engine-default.js package.json ${dir}

# Set the npm registry
ARG NPM_REGISTRY=https://registry.npmjs.org/
RUN npm config set @comunica:registry $NPM_REGISTRY

# Install the node module
RUN cd ${dir} && npm install --only=production --ignore-scripts

RUN cp -r /var/www/@comunica/query-sparql-link-traversal/node_modules/@comunica/config-query-sparql/config/rdf-metadata-accumulate/ /var/www/@comunica/query-sparql-link-traversal/node_modules/rdf-parse/node_modules/@comunica/config-query-sparql/config/

RUN cd ${dir} && npm run prepare

# Run base binary (generated from package.json!bin)
WORKDIR ${dir}
ENTRYPOINT ["node", "./bin/query.js"]

# Default command
CMD ["--help"]
