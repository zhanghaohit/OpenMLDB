# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


FROM centos:7

ARG ZETASQL_VERSION=0.3.4
ARG THIRDPARTY_VERSION=0.7.0
ARG TARGETARCH

LABEL org.opencontainers.image.source https://github.com/4paradigm/OpenMLDB

COPY ./*.sh /
# hadolint ignore=DL3031,DL3033
RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo && \
    sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo && \
    sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo && \
    yum update -y && yum install -y centos-release-scl epel-release && \
    /patch_yum_repo.sh && \
    yum install -y devtoolset-8 rh-git227 devtoolset-8-libasan-devel flex doxygen java-1.8.0-openjdk-devel rh-python38-python-devel rh-python38-python-wheel rh-python38-python-requests rh-python38-python-pip && \
    curl -Lo lcov-1.15-1.noarch.rpm https://github.com/linux-test-project/lcov/releases/download/v1.15/lcov-1.15-1.noarch.rpm && \
    yum localinstall -y lcov-1.15-1.noarch.rpm && \
    yum clean all && rm -v lcov-1.15-1.noarch.rpm && \
    curl -Lo zookeeper.tar.gz https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz && \
    mkdir -p /deps/src && \
    tar xzf zookeeper.tar.gz -C /deps/src && \
    rm -v ./*.tar.gz && \
    /setup_deps.sh -a "$TARGETARCH" -z "$ZETASQL_VERSION" -t "$THIRDPARTY_VERSION" && \
    rm -v /*.sh

ENV THIRD_PARTY_DIR=/deps/usr
ENV THIRD_PARTY_SRC_DIR=/deps/src
ENV PATH=/opt/rh/rh-git227/root/usr/bin:/opt/rh/rh-python38/root/usr/local/bin:/opt/rh/rh-python38/root/usr/bin:/opt/rh/devtoolset-8/root/usr/bin:/deps/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LD_LIBRARY_PATH=/deps/usr/lib:/deps/usr/lib64:/opt/rh/httpd24/root/usr/lib64:/opt/rh/rh-python38/root/usr/lib64:/opt/rh/devtoolset-8/root/usr/lib64:/opt/rh/devtoolset-8/root/usr/lib:/opt/rh/devtoolset-8/root/usr/lib64/dyninst
ENV LANG=en_US.UTF-8
ENV ZETASQL_VERSION=$ZETASQL_VERSION
ENV THIRDPARTY_VERSION=$THIRDPARTY_VERSION

WORKDIR /root

CMD [ "/bin/bash" ]

