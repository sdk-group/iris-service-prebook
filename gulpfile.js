'use strict'

let gulp = require("gulp");
let sourcemaps = require("gulp-sourcemaps");
let babel = require("gulp-babel");
let watch = require('gulp-watch');
let changed = require('gulp-changed');
let nodemon = require('gulp-nodemon');
let plumber = require('gulp-plumber');
let path = require('path');
let demon;


gulp.task("default", ['es6']);

gulp.task("sourcemaps", function() {
	return gulp.src("src/**/*.js")
		.pipe(sourcemaps.init())
		.pipe(babel())
		.pipe(sourcemaps.write("./maps"))
		.pipe(gulp.dest("build"));
});

gulp.task("es6-js", function() {
	return gulp.src(["src/**/*.js", "tests/**/*.js"])
		.pipe(changed("build"))
		.pipe(plumber({
			errorHandler: function(e) {
				console.log('error', e);
			}
		}))
		.pipe(babel())
		.pipe(gulp.dest("build"))
		.on('end', function() {
			console.log('end build');
		});
});

gulp.task("json", function() {
	return gulp.src(["src/**/*.json", "tests/**/*.json"])
		.pipe(gulp.dest("build"));
});

gulp.task('es6', ['es6-js', 'json']);

gulp.task('upd', ['es6'], function() {
	return gulp.src(["build/**/*.js"])
		.pipe(gulp.dest("../iris-v2/node_modules/iris-service-prebook/build"));
});

gulp.task('test-upd', ['start-test'], function() {
	gulp.watch(["src/**/*.js", "tests/**/*.js"], ['upd']);
});

gulp.task('test', ['start-test'], function() {
	gulp.watch(["src/**/*.js", "tests/**/*.js"], ['es6']);
});

gulp.task('serve', ['start-serve'], function() {
	gulp.watch(["src/**/*.js", "tests/**/*.js"], ['es6']);
});

gulp.task('start-test', function() {
	demon = nodemon({
		script: 'build/run.js',
		watch: ['build/'],
		execMap: {
			"js": "node  --harmony --harmony_proxies"
		},
		env: {
			'NODE_ENV': 'development'
		}
	});
});

gulp.task('start-serve', function() {
	demon = nodemon({
		script: 'build/index.js',
		watch: ['build/'],
		execMap: {
			"js": "node  --harmony --harmony_proxies"
		},
		env: {
			'NODE_ENV': 'development'
		}
	});
});