module.exports = {
	name: 'prebook_slots_count',
	compute: function (datapart) {
		let part = datapart.prebook_percentage;
		// console.log("prebook mapping", datapart.prebook_chunk_mapping);
		// let max_slots = _.floor(_.min([datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved)]) / datapart.prebook_slot_size);
		// console.log("MAXSL", max_slots, datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved));
		let val = (datapart.forced_max_prebook_slots_count - datapart.forced_prebook_slots_count);
		return _.isNumber(val) ? val : _.clamp(_.floor(_.sumBy(datapart.prebook_chunk_mapping, t => (_.floor(t / datapart.prebook_slot_size) - 1)) * part), 0, Infinity);
	}

};