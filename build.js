'use strict'

let gulp = require("gulp");
let babel = require("gulp-babel");

gulp.task("es6-js", function() {
	return gulp.src(["src/**/*.js", "tests/**/*.js"])
		.pipe(babel({
			"presets": [
				"es2015-node5"
			],
			"babelrc": false
		}))
		.pipe(gulp.dest("build"))
		.on('end', function() {
			console.log('end build');
		});
});

gulp.task("json", function() {
	return gulp.src(["src/**/*.json"])
		.pipe(gulp.dest("build"));
});

gulp.task('default', ['es6-js', 'json']);