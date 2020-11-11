CC      = gcc
CXX     = g++
INSTALL = install
LDFLAGS = -lcurl -lrt -ldl -lpthread -lz -lcrypto
DEPSDIR = ".deps"
ARLIBS  = $(DEPSDIR)/librdkafka.a $(DEPSDIR)/libluajit-5.1.a $(DEPSDIR)/libjsoncpp.a $(DEPSDIR)/libhs.a
CFLAGS  += -I/usr/local/include/luajit-2.0 $(PARAM_CFLAGS)
PREDEF  += $(PARAM_PREDEF)
WARN    = -Werror -Wall -Wshadow -Wextra -Wno-comment -Wno-deprecated-declarations -Wno-format-truncation -Wno-format-overflow -Wno-literal-suffix

ifeq ($(DEBUG), 1)
	CFLAGS += -O0 -g
else
	CFLAGS += -O2 -g
endif

ifndef ($(INSTALLDIR))
	INSTALLDIR = /usr/local
endif

VPATH = .:./libs
BUILDDIR = build

OBJ = $(BUILDDIR)/common.o $(BUILDDIR)/cnfctx.o $(BUILDDIR)/luactx.o $(BUILDDIR)/transform.o \
      $(BUILDDIR)/filereader.o $(BUILDDIR)/inotifyctx.o $(BUILDDIR)/fileoff.o $(BUILDDIR)/cmdnotify.o \
      $(BUILDDIR)/luafunction.o $(BUILDDIR)/kafkactx.o $(BUILDDIR)/sys.o $(BUILDDIR)/util.o \
      $(BUILDDIR)/esctx.o $(BUILDDIR)/metrics.o $(BUILDDIR)/taskqueue.o

default: configure tail2kafka kafka2file tail2kafka_unittest tail2es_unittest kafka2file_unittest
	@echo finished

tail2kafka: $(BUILDDIR)/tail2kafka.o $(OBJ)
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

tail2kafka_unittest: $(BUILDDIR)/tail2kafka_unittest.o $(OBJ)
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

tail2es_unittest: $(BUILDDIR)/tail2es_unittest.o $(OBJ)
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

kafka2file_unittest: $(BUILDDIR)/kafka2file_unittest.o $(OBJ)
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

kafka2file: $(BUILDDIR)/kafka2file.o $(OBJ)
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

speedlimit: $(BUILDDIR)/mix/speedlimit.o
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^

.PHONY: get-deps
get-deps:
	@mkdir -p $(DEPSDIR)

	@echo "compile jsoncpp" && \
	  cd $(DEPSDIR) && \
	  (test -f 0.10.4.tar.gz || wget https://github.com/open-source-parsers/jsoncpp/archive/0.10.4.tar.gz) && \
		rm -rf jsoncpp-0.10.4 && tar xzf 0.10.4.tar.gz &&   \
	  mkdir -p jsoncpp-0.10.4/build && cd jsoncpp-0.10.4/build && \
	  cmake -DCMAKE_CXX_FLAGS="-Wno-deprecated-declarations -Wno-implicit-fallthrough -Wno-shift-negative-value" -DCMAKE_BUILD_TYPE=debug -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF \
	    -DARCHIVE_INSTALL_DIR=../.. -G "Unix Makefiles" .. && make install

	@echo "compile librdkafka" && \
	  cd $(DEPSDIR) && \
    (test -f v0.11.3.tar.gz || wget https://github.com/edenhill/librdkafka/archive/v0.11.3.tar.gz) && \
	  rm -rf librdkafka-0.11.3 && tar xzf v0.11.3.tar.gz && cd librdkafka-0.11.3 && \
    ./configure --disable-ssl --disable-sasl && make -j2 && make install
	cp /usr/local/lib/librdkafka.a $(DEPSDIR)

	@echo "compile libluajit" && \
	  cd $(DEPSDIR) && \
    (test -f LuaJIT-2.0.4.tar.gz || wget http://luajit.org/download/LuaJIT-2.0.4.tar.gz) && \
     rm -rf LuaJIT-2.0.4 && tar xzf LuaJIT-2.0.4.tar.gz && cd LuaJIT-2.0.4 && \
	   make -j2 && make install
	cp /usr/local/lib/libluajit-5.1.a $(DEPSDIR)

.PHONY: configure
configure:
	@mkdir -p $(BUILDDIR)
	@mkdir -p $(BUILDDIR)/mix
	@ls -l $(ARLIBS) >/dev/null || (echo "make get-deps first" && exit 2)

.PHONY: debug
debug:
	make DEBUG=1

$(BUILDDIR)/%.o: src/%.cc
	$(CXX) -o $@ $(WARN) $(CXXWARN) $(CFLAGS) $(PREDEF) -c $<

$(BUILDDIR)/mix/%.o: mix/%.cc
	$(CXX) -o $@ $(WARN) $(CXXWARN) $(CFLAGS) $(PREDEF) -c $<

tail2kafka_blackbox: $(BUILDDIR)/tail2kafka_blackbox.o
	$(CXX) $(CFLAGS) -o $(BUILDDIR)/$@ $^ $(ARLIBS) $(LDFLAGS)

.PHONY: test
test:
	mkdir -p logs kafka2filedir

	@echo "unit test"
	find logs -type f -name "*.log" -delete
	find kafka2filedir -type f -delete
	make clean && make PARAM_PREDEF="-D_DEBUG_" DEBUG=1
	$(BUILDDIR)/tail2kafka_unittest
	$(BUILDDIR)/tail2es_unittest
	$(BUILDDIR)/kafka2file_unittest

	@echo "blackbox test"
	make clean && make PARAM_PREDEF="-D_DEBUG_" && make tail2kafka_blackbox
	./blackboxtest/blackbox_test.sh

.PHONY: install
install:
	$(INSTALL) -D tail2kafka $(RPM_BUILD_ROOT)$(INSTALLDIR)/bin
	mkdir -p $(RPM_BUILD_ROOT)/etc/tail2kafka
	mkdir -p $(RPM_BUILD_ROOT)/var/lib/

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
