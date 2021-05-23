SHELL = bash
export OPENBLAS_NUM_THREADS=1
export SPARK_VERSION=3.1.1
export SBT_OPTS=-Xmx16G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M

console:
	sbt console
	
console-quick:
	sbt consoleQuick
	
compile:
	sbt compile
	
package:
	sbt package
	
publish-snapshot:
	sbt publishSigned
	sbt sonatypePrepare
	sbt sonatypeBundleUpload
	sbt sonatypeReleaseAll

publish:
	sbt publishSigned
	sbt sonatypeBundleRelease

build-classpath-file:
	bash sbin/deps.sh
