#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

version=${CASSANDRA_VERSION:-"3.11.12.3-SNAPSHOT"}
# Deploy snapshot artifacts into repository https://oss.sonatype.org/content/repositories/snapshots
# Deploy release artifacts into the staging repository https://oss.sonatype.org/service/local/staging/deploy/maven2
# Download snapshot and release artifacts from group https://oss.sonatype.org/content/groups/public
# Download snapshot, release and staged artifacts from staging group https://oss.sonatype.org/content/groups/staging

if [[ $version == *SNAPSHOT ]]
then
   url=https://oss.sonatype.org/content/repositories/snapshots
else
   url=https://oss.sonatype.org/service/local/staging/deploy/maven2
   opt="-Dapache-release"
fi

mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=pom \
    -Dfile=build/strapdata-cassandra-$version-parent.pom \
    -DpomFile=build/strapdata-cassandra-$version-parent.pom \
    $opt

# cassandra-thrift jar
mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-thrift-$version-sources.jar \
    -DpomFile=build/strapdata-cassandra-thrift-$version.pom \
    $opt -Dclassifier=sources

mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-thrift-$version-javadoc.jar \
    -DpomFile=build/strapdata-cassandra-thrift-$version.pom \
    $opt -Dclassifier=javadoc

mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-thrift-$version.jar \
    -DpomFile=build/strapdata-cassandra-thrift-$version.pom \
    $opt

# cassandra-clientutil jar
#mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
#    -DrepositoryId=ossrh -Durl=$url \
#    -Dpackaging=jar \
#    -Dfile=build/strapdata-cassandra-clientutil-$version.jar \
#    -DpomFile=build/strapdata-cassandra-clientutil-$version.pom \
#    -Dapache-release

#mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
#    -DrepositoryId=ossrh -Durl=$url \
#    -Dpackaging=jar \
#    -Dfile=build/strapdata-cassandra-clientutil-$version-sources.jar \
#    -DpomFile=build/strapdata-cassandra-clientutil-$version.pom \
#    -Dapache-release -Dclassifier=sources

#mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
#    -DrepositoryId=ossrh -Durl=$url \
#    -Dpackaging=jar \
#    -Dfile=build/strapdata-cassandra-clientutil-$version-javadoc.jar \
#    -DpomFile=build/strapdata-cassandra-clientutil-$version.pom \
#    -Dapache-release -Dclassifier=javadoc

# the strapdata-cassandra-all jar
mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-$version-sources.jar \
    -DpomFile=build/strapdata-cassandra-$version.pom \
    $opt -Dclassifier=sources

mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-$version-javadoc.jar \
    -DpomFile=build/strapdata-cassandra-$version.pom \
    $opt -Dclassifier=javadoc

mvn -X org.apache.maven.plugins:maven-gpg-plugin:1.4:sign-and-deploy-file \
    -DrepositoryId=ossrh -Durl=$url \
    -Dpackaging=jar \
    -Dfile=build/strapdata-cassandra-$version.jar \
    -DpomFile=build/strapdata-cassandra-$version.pom \
    $opt

