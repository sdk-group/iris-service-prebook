module.exports = {
	name: 'prebook_slots_count',
	compute: function (datapart) {
		let part = datapart.part;
		let real_part = datapart.reserved / datapart.max_available;
		part = part - real_part;
		// console.log("prebook mapping", datapart.prebook_chunk_mapping);
		// let max_slots = _.floor(_.min([datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved)]) / datapart.prebook_slot_size);
		// console.log("MAXSL", max_slots, datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved));
		let val = (datapart.forced_max_prebook_slots_count - datapart.forced_prebook_slots_count);
		return _.isNumber(datapart.forced_max_prebook_slots_count) ? val : _.clamp(_.floor(datapart.prebook_slots_count * part), 0, Infinity);
	}

};