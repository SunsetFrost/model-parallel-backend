FROM uhopper/hadoop:2.8.1

ENV SPARK_VERSION 2.4.0

ENV SPARK_BIN_URL https://mirrors.cnnic.cn/apache/spark/spark-2.4.0/spark-2.4.0-bin-without-hadoop.tgz

ENV SPARK_HOME=/opt/spark-$SPARK_VERSION
ENV WINE_HOME=/opt/wine 

# install wine
WORKDIR $WINE_HOME
ADD winehq.key ./
ADD sources.list ./
ADD libcurl3-gnutls_7.38.0-4+deb8u11_amd64.deb ./

RUN dpkg -i ./libcurl3-gnutls_7.38.0-4+deb8u11_amd64.deb \
    && apt install -y apt-transport-https \
    && mv -f ./sources.list /etc/apt/  \
    && dpkg --add-architecture i386 \
    && apt-key add ./winehq.key \
    && apt update \
    && apt install --install-recommends -y winehq-stable

# install spark

RUN set -x \
    && curl -fSL "$SPARK_BIN_URL" -o /tmp/spark.tar.gz \
    && tar -xvf /tmp/spark.tar.gz -C /opt \
    && mv /opt/spark-$SPARK_VERSION-* $SPARK_HOME \
    && rm -f /tmp/spark.tar.gz

WORKDIR $SPARK_HOME
ENV PATH $SPARK_HOME/bin:$PATH

ADD spark-entrypoint.sh /
ADD spark-historyserver.sh /
ADD spark-master.sh /
ADD spark-slave.sh /

RUN chmod a+x \
    /spark-entrypoint.sh \
    /spark-historyserver.sh \
    /spark-master.sh \
    /spark-slave.sh

RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> /opt/spark-$SPARK_VERSION/conf/spark-env.sh     

# install python essential
RUN apt install -y python3 \
    && ln -s /usr/bin/python3.4 /usr/bin/python

ENTRYPOINT ["/spark-entrypoint.sh"]