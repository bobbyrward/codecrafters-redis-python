from dataclasses import dataclass
import asyncio
import time


@dataclass
class Command:
    command: str
    args: list


class Cache:
    def __init__(self):
        self.store = {}
        self.timeouts = {}

    def get(self, key):
        if key in self.store:
            timeout = self.timeouts.get(key)

            print(f"Key '{key}' has timeout '{timeout}'. Current time is {time.time()}")
            if timeout and time.time() > timeout:
                del self.store[key]
                del self.timeouts[key]
                return b"$-1\r\n"
            else:
                value = self.store[key]
                prefix = b"$" + str(len(value)).encode("ascii") + b"\r\n"
                encoded_value = value + b"\r\n"

                return b"".join((prefix, encoded_value))
        else:
            return b"$-1\r\n"

    def set(self, key, value):
        self.store[key] = value

    def set_timeout(self, key, value):
        self.timeouts[key] = time.time() + (value / 1000.0)
        print(f"Current time is {time.time()}")
        print(f"Timeout is      {self.timeouts[key]}")


CACHE = Cache()


async def read_bulk_string(reader):
    length = int((await reader.readuntil(separator=b"\r\n"))[:2])
    value = await reader.read(length)
    await reader.read(2)
    return value


async def read_simple_string(reader):
    (await reader.readuntil(separator=b"\r\n"))[:2]


async def read_error_string(reader):
    (await reader.readuntil(separator=b"\r\n"))[:2]


async def read_integer(reader):
    int((await reader.readuntil(separator=b"\r\n"))[:2])


async def read_type(reader):
    resp_type = await reader.read(1)

    type_map = {
        b"*": read_array,
        b"$": read_bulk_string,
        b"+": read_simple_string,
        b"-": read_error_string,
        b":": read_integer,
    }

    try:
        type_reader = type_map[resp_type]
    except KeyError:
        raise Exception(f"Unknown RESP type: {resp_type}")

    return await type_reader(reader)


async def read_array(reader):
    array_len = int((await reader.readuntil(separator=b"\r\n"))[:2])
    parts = []

    for index in range(array_len):
        array_part = await read_type(reader)

        parts.append(array_part)

    return parts


async def read_command(reader):
    resp_type = await reader.read(1)
    if not resp_type:
        return None

    if resp_type != b"*":
        raise Exception(f"commands should be arrays not {resp_type}")

    command_array = await read_array(reader)

    return Command(command=command_array[0], args=command_array[1:])


async def handle_ping(writer, args):
    response = "PONG"

    if not args:
        response = b"PONG"
    elif len(args) == 1:
        response = args[0]
    else:
        raise Exception(f"PING has wrong arguments: {args}")

    writer.write(b"".join((b"+", response, b"\r\n")))


async def handle_echo(writer, args):
    if len(args) != 1:
        raise Exception(f"ECHO has wrong arguments: {args}")

    writer.write(b"".join((b"+", args[0], b"\r\n")))


async def handle_get(writer, args):
    if len(args) != 1:
        raise Exception(f"GET has wrong arguments: {args}")

    writer.write(CACHE.get(args[0]))


async def handle_set(writer, args):
    args_len = len(args)

    if args_len < 2:
        raise Exception(f"SET has wrong arguments: {args}")

    key = args[0]
    value = args[1]

    if args_len == 2:
        timeout = None
    elif args_len == 4:
        if args[2] != b"PX":
            raise Exception(f"SET has wrong arguments: {args}")

        timeout = int(args[3])
    else:
        raise Exception(f"SET has wrong arguments: {args}")

    CACHE.set(key, value)

    if timeout:
        CACHE.set_timeout(key, timeout)

    writer.write(b"+OK\r\n")


COMMAND_MAP = {
    b"PING": handle_ping,
    b"ECHO": handle_echo,
    b"GET": handle_get,
    b"SET": handle_set,
}


async def handle_connection(reader, writer):
    while True:
        command = await read_command(reader)

        if not command:
            print("Client disconnected")
            break

        print(f"COMMAND = '{command}'")

        try:
            command_handler = COMMAND_MAP[command.command.upper()]
        except KeyError:
            print(f"Unknown command: {command.command}")
            break

        await command_handler(writer, command.args)


def main():
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(
        asyncio.start_server(handle_connection, host="127.0.0.1", port=6379, reuse_address=True)
    )

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    main()
