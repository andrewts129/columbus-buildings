FROM node:8.16.0-slim

ENV NODE_ENV="production"

RUN apt-get -qq update \
&& DEBIAN_FRONTEND=noninteractive apt-get -y install \
    apt-transport-https \
    curl \
    unzip \
    build-essential \
    python \
    libcairo2-dev \
    libgles2-mesa-dev \
    libgbm-dev \
    libllvm3.9 \
    libprotobuf-dev \
    libxxf86vm-dev \
    xvfb \
    x11-utils \
    git \
&& apt-get clean

RUN mkdir -p /usr/src/app && \
    git clone https://github.com/klokantech/tileserver-gl.git /usr/src/app && \
    cd /usr/src/app && \
    npm install --production

COPY . /data

EXPOSE 80
CMD bash /usr/src/app/run.sh
