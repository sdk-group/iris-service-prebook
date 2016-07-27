module.exports = {
	name: 'prebook_slots_count',
	compute: function (datapart) {
		let part = datapart.prebook_percentage;
		return _.clamp(_.floor(part * _.sumBy(datapart.prebook_chunk_mapping, t => (_.floor(t / datapart.prebook_slot_size) - 1))), 0, Infinity);
	}

};