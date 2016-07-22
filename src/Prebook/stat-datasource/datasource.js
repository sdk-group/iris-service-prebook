'use strict'
class CachingDatasource {
	constructor(bucket) {
		this._bucket = bucket;
		this._dataset = {};
	}

	flush() {
		this._dataset = {};
	}

	get({
		keys,
		query,
		options
	}) {
		// console.log("DSDP GET", keys, query, options);
		if (query) {
			switch (query.type) {
			case 'chain':
				return this.process_chain(query, options);
				break;
			default:
				return {};
				break;
			}
		}
		return this._bucket.getNodes(keys, options);
	}

	getNodes(compound_key, options) {
		let p = {};
		let flatten = (obj) => {
			if (_.isArray(obj) || _.isString(obj)) {
				return this._bucket.getNodes(obj, options);
			} else {
				return Promise.props(_.reduce(obj, (acc, val, k) => {
					acc[k] = flatten(val);
					return acc;
				}, {}));
			}
		};

		let k = compound_key.templates ? compound_key.keys : compound_key;
		p = flatten(k);

		// console.log("CMP", compound_key, k);
		return compound_key.templates ? Promise.props({
			keys: p,
			templates: compound_key.templates
		}) : p;
	}

	process_chain(q, options) {
		return Promise.reduce(q.query, (acc, query, index) => {
				let keys = query.in_keys || query.out_keys(acc[index - 1].nodes);
				let cached = _.pick(this._dataset, keys) || {};

				let [nonex_keys, ex_keys] = _.partition(keys, k => (_.isUndefined(cached[k]) || !!cached[k].error));
				// console.log("CACHE", _.size(ex_keys), "\nUNDEFINED ", _.size(nonex_keys), "\n\n______________________________________________________________________");
				return this._bucket.getNodes(nonex_keys, options)
					.then((nodes) => {
						// console.log("NODES", index, nodes);
						acc[index] = {
							name: query.name,
							nodes: _.merge(_.cloneDeep(cached), nodes)
						};
						return acc;
					});
			}, [])
			.then((res) => {
				let out = _(res)
					.keyBy('name')
					.mapValues((t, qname) => _(t.nodes)
						.values()
						.filter((v, k) => {
							let key = _.get(v, 'value.@id');
							let type = _.get(v, 'value.@type');
							if (type != 'Plan' && !_.isUndefined(type) && !_.isUndefined(key)) {
								this._dataset[key] = v;
							}
							return !_.isUndefined(v);
						})
						.value())
					.value();
				// console.log("OUTTT--------------------------------------->", out);
				return _.isFunction(q.final) ? q.final(out) : out;
			});
	}
}


module.exports = CachingDatasource;