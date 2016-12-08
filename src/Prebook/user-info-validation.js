'use strict';

const Patchwerk = require('patchwerk');

function discover(name) {
	try {
		return require(`./validator/${_.kebabCase(name)}.js`);
	} catch (err) {
		return false;
	}
}

class UserInfoValidation {
	constructor(message_bus, method) {
		this.patchwerk = Patchwerk(message_bus);
		this.emitter = message_bus;
		this._known_validators = {};
	}


	// methods
	_fieldLimitations(user_info) {
		let keys = _.map(user_info, (value, key) => this._limitationKey(key, value));
		return this.emitter.addTask("database.getMulti", {
				args: [keys]
			})
			.then(res => _.mapValues(res, r => r.value ? r : false));
	}

	_limitationKey(key, value) {
		return `user-info-limitation-${key}-${value}`;
	}

	_validator(name) {
		if (!this._known_validators[name]) {
			this._known_validators[name] = discover(name);
		}
		return this._known_validators[name];
	}

	validate(data) {
		let user_info = data.user_info;
		return Promise.props({
				uif: this.patchwerk.get("user-info-fields", {
					key: "user_info_fields"
				}),
				limitations: this._fieldLimitations(user_info)
			})
			.then(({
				uif: uif_obj,
				limitations: limitations
			}) => {
				console.log("UIF & LIMS", uif_obj, limitations);
				let req = uif_obj.requiredValidation();
				console.log(req);
				let fields = Object.keys(req),
					l = fields.length,
					validator, ll, curr_v, res = true,
					reason;
				for (var i = 0; i < l; i++) {
					ll = req[fields[i]].length;
					for (var j = 0; j < ll; j++) {
						curr_v = req[fields[i]][j];
						validator = this._validator(curr_v.type);
						if (!validator)
							continue;
						res = res && validator.validate(curr_v.params, limitations[this._limitationKey(fields[i], user_info[fields[i]])], data);
						!res && (reason = fields[i]);
						if (!res)
							break;
					}
					if (!res)
						break;
				}
				if (!res)
					return Promise.reject(new Error("User info validation failed: " + reason));
				return res;
			});
	}

	update(data) {
		let user_info = data.user_info;
		return Promise.props({
				uif: this.patchwerk.get("user-info-fields", {
					key: "user_info_fields"
				}),
				limitations: this._fieldLimitations(user_info)
			})
			.then(({
				uif: uif_obj,
				limitations: limitations
			}) => {
				console.log("TOSAVE", limitations);
				let req = uif_obj.requiredValidation();
				let fields = Object.keys(req),
					l = fields.length,
					validator, ll, curr_v, res = [];
				for (var i = 0; i < l; i++) {
					ll = req[fields[i]].length;
					for (var j = 0; j < ll; j++) {
						curr_v = req[fields[i]][j];
						validator = this._validator(curr_v.type);
						if (!validator)
							continue;
						let lk = this._limitationKey(fields[i], user_info[fields[i]]);
						let node = validator.update(curr_v.params, limitations[lk], data);
						node["@id"] = lk;
						node["@type"] = "Description";

						res.push(node);
					}
				}
				console.log("UIRES", res);
				return this.emitter.addTask('database.upsertNodes', {
					args: [res]
				});
			});
	}
}

module.exports = function (...args) {
	return new UserInfoValidation(...args);
};