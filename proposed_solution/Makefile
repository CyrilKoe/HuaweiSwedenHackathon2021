CC       := gcc
CFLAGS   := -Wall -MD -MP -g
LDFLAGS  := -ljson-c -lm
BUILD    := ./build
OBJ_DIR  := $(BUILD)/objects
APP_DIR  := $(BUILD)/apps
TARGET   := App
INCLUDE  := -I.

SRC      := $(wildcard *.c)

OBJECTS  := $(SRC:%.c=$(OBJ_DIR)/%.o)

all: build $(APP_DIR)/$(TARGET)

-include $(subst .o,.d,$(OBJECTS))

$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@ $(LDFLAGS)

$(APP_DIR)/$(TARGET): $(OBJECTS)
	ln -s ../testcases testcases || true
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -o $(APP_DIR)/$(TARGET) $^ $(LDFLAGS)

.PHONY: all build clean debug release

build:
	@mkdir -p $(APP_DIR)
	@mkdir -p $(OBJ_DIR)

debug: CXXFLAGS += -DDEBUG -g
debug: all

run: all
	$(APP_DIR)/$(TARGET) 1
	$(APP_DIR)/$(TARGET) 2
	$(APP_DIR)/$(TARGET) 3
	$(APP_DIR)/$(TARGET) 4
	$(APP_DIR)/$(TARGET) 5
	$(APP_DIR)/$(TARGET) 6
	$(APP_DIR)/$(TARGET) 7
	$(APP_DIR)/$(TARGET) 8
	$(APP_DIR)/$(TARGET) 9
	$(APP_DIR)/$(TARGET) 10
	$(APP_DIR)/$(TARGET) 11
	$(APP_DIR)/$(TARGET) 12
	

clean:
	-@rm -rvf $(OBJ_DIR)/*
	-@rm -rvf $(APP_DIR)/*