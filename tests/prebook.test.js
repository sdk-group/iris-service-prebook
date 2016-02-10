'use strict'

let Prebook = require("./Prebook/prebook");
let config = require("./config/db_config.json");

describe("Prebook service", () => {
	let service = null;
	let bucket = null;
	before(() => {
		service = new Prebook();
		service.init();
	});
	describe("Prebook service", () => {
		it("should get next ticket", (done) => {
			return service.getNextTicket()
				.then((res) => {
					done();
				})
				.catch((err) => {
					done(err);
				});
		})
		it("should get ticket by id", (done) => {
			return service.getTicketById()
				.then((res) => {
					console.log(res);
					done();
				})
				.catch((err) => {
					done(err);
				});
		})
		it("should get prebook page", (done) => {
			return service.getPrebookPage()
				.then((res) => {
					done();
				})
				.catch((err) => {
					done(err);
				});
		})
		it("should get prebook length", (done) => {
			return service.getPrebookLength()
				.then((res) => {
					done();
				})
				.catch((err) => {
					done(err);
				});
		})
		it("should check waiting", (done) => {
			return service.checkWaiting()
				.then((res) => {
					console.log(res);
					done();
				})
				.catch((err) => {
					done(err);
				});
		})
	})

});