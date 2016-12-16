CC      = gcc
CXX     = g++
INSTALL = install
LDFLAGS = -lrt -ldl -lpthread -lz
PREDEF  =
ARLIBS  = ./deps/librdkafka.a ./deps/libluajit-5.1.a
CFLAGS  = -I/usr/local/include/luajit-2.0

ifeq ($(DEBUG), 1)
	CFLAGS += -O0 -g
	WARN   += -Wall -Wextra -Wno-comment -Wformat -Wimplicit \
            -Wparentheses -Wswitch -Wunused
	PREDEF += -DDEBUG
else
	CFLAGS += -O2 -g
	WARN   += -Wall -Wextra -Wno-comment -Wformat -Wimplicit \
            -Wparentheses -Wswitch -Wuninitialized -Wunused
endif

ifeq ($(UNITTEST), 1)
	PREDEF += -DUNITTEST -DNO_LOGER
endif

ifndef ($(INSTALLDIR))
	INSTALLDIR = /usr/local
endif

VPATH = .:./libs

default: configure tail2kafka
	@echo finished

tail2kafka: build/tail2kafka.o
	$(CXX) $(CFLAGS) -o $@ $^ $(ARLIBS) $(LDFLAGS)

unittest:
	make clean && mkdir -p logs && make UNITTEST=1 && ./tail2kafka && rm -rf logs && make clean

.PHONY: get-deps
get-deps:
	@mkdir -p deps

	@echo "compile librdkafka" && \
	  cd deps && \
    (test -f v0.9.2 || wget https://github.com/edenhill/librdkafka/archive/v0.9.2.tar.gz) && \
	  rm -rf librdkafka-0.9.2 && tar xzf v0.9.2 && cd librdkafka-0.9.2 && \
    ./configure --disable-ssl --disable-sasl && make -j2 && make install
	cp /usr/local/lib/librdkafka.a ./deps

	@echo "compile libluajit" && \
	  cd deps && \
    (test -f LuaJIT-2.0.4.tar.gz || wget http://luajit.org/download/LuaJIT-2.0.4.tar.gz) && \
     rm -rf LuaJIT-2.0.4 && tar xzf LuaJIT-2.0.4.tar.gz && cd LuaJIT-2.0.4 && \
	   make -j2 && make install
	cp /usr/local/lib/libluajit-5.1.a ./deps

.PHONY: configure
configure:
	@mkdir -p build
	@ls -l $(ARLIBS) >/dev/null || (echo "make get-deps first" && exit 2)

build/%.o: src/%.cc
	$(CXX) -o $@ $(WARN) $(CXXWARN) $(CFLAGS) $(PREDEF) -c $<

.PHONY: debug
debug:
	make DEBUG=1

.PHONY: test
test:
	@echo "if first run test, run make clean"
	make debug

.PHONY: install
install:
	$(INSTALL) -D tail2kafka           $(INSTALLDIR)/bin
	mkdir -p /etc/tail2kafka

.PHONY: clean
clean:
	rm -rf ./build/*
