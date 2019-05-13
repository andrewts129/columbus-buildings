FROM node:6.15.1-stretch

ENV NODE_ENV="production"
EXPOSE 80

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

CMD bash /usr/src/app/run.sh
