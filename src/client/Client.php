<?php

/**
 * MIT License
 *
 * Copyright (c) 2024 cooldogedev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * @auto-license
 */

declare(strict_types=1);

namespace cooldogedev\Spectrum\client;

use cooldogedev\spectral\Stream;
use cooldogedev\Spectrum\client\packet\ProxyPacketIds;
use pocketmine\network\mcpe\protocol\DataPacket;
use pocketmine\network\mcpe\protocol\ProtocolInfo;
use pocketmine\network\mcpe\protocol\serializer\CommonTypes;
use pocketmine\network\mcpe\raklib\SnoozeAwarePthreadsChannelWriter;
use pocketmine\thread\log\ThreadSafeLogger;
use pocketmine\utils\Binary;
use pocketmine\utils\BinaryDataException;
use Socket;
use pmmp\encoding\LE;
use pmmp\encoding\ByteBufferReader;
use function snappy_compress;
use function snappy_uncompress;
use function socket_close;
use function socket_read;
use function socket_write;
use function socket_last_error;
use function socket_strerror;
use function strlen;
use function substr;
use const SOCKET_EWOULDBLOCK;

final class Client {

    private const PACKET_LENGTH_SIZE = 4;
    private const COMPRESSION_THRESHOLD = 256;

    private const FLAG_PACKET_DECODE_NEEDED = 1 << 0;
    private const FLAG_PACKET_COMPRESSED = 1 << 1;
    private const FLAG_PACKET_BATCHED = 1 << 2;

    private string $buffer = "";

    private ?int $expected = ProxyPacketIds::CONNECTION_REQUEST;
    private int $length = 0;

    private bool $closed = false;

    public function __construct(
        public readonly Socket                           $socket,
        public readonly ThreadSafeLogger                 $logger,
        public readonly SnoozeAwarePthreadsChannelWriter $writer,
        public readonly int                              $id,
    ) {}

    public function isClosed(): bool {
        return $this->closed;
    }

    public function tick(): void
    {
        if ($this->closed) {
            return;
        }

        // Read data from socket
        $data = @socket_read($this->socket, 65535);
        if ($data === false) {
            $error = socket_last_error($this->socket);
            if ($error === SOCKET_EWOULDBLOCK) {
                // No data available yet, this is normal for non-blocking sockets
                return;
            }
            // Real error occurred
            $this->logger->debug("Socket read failed: " . socket_strerror($error));
            $this->close();
            return;
        }
        
        if ($data === "") {
            // Connection closed by peer
            $this->logger->debug("Connection closed by peer");
            $this->close();
            return;
        }

        $this->buffer .= $data;
        $this->read();
    }

    public function read(): void
    {
        if ($this->closed) {
            return;
        }

        if ($this->length === 0 && strlen($this->buffer) >= Client::PACKET_LENGTH_SIZE) {
            try {
                $length = Binary::readInt($this->buffer);
            } catch (BinaryDataException) {
                return;
            }
            $this->length = $length;
            $this->buffer = substr($this->buffer, Client::PACKET_LENGTH_SIZE);
        }

        if ($this->length === 0 || $this->length > strlen($this->buffer)) {
            return;
        }

        // Parse the flags and determine whether the packet needs to be compressed.
        $flags = Binary::readByte($this->buffer[0]);
        $needsCompression = ($flags & Client::FLAG_PACKET_COMPRESSED) !== 0;
        $isBatch = ($flags & Client::FLAG_PACKET_BATCHED) !== 0;
        $payload = $needsCompression ? 
            @snappy_uncompress(substr($this->buffer, 1, $this->length - 1)) : 
            substr($this->buffer, 1, $this->length - 1);
        if ($payload !== false) {
			$isBatch ? $this->handleBatch($payload) : $this->handlePacket($payload);
        } else {
            $this->logger->debug("Failed to decompress/parse payload. Length: " . $this->length . ", Buffer size: " . strlen($this->buffer));
            $this->close();
        }

        $this->buffer = substr($this->buffer, $this->length);
        $this->length = 0;
        if (strlen($this->buffer) >= Client::PACKET_LENGTH_SIZE) {
            $this->read();
        }
    }

    private function handleBatch(string $payload): void {
        $reader = new ByteBufferReader($payload);
        while ($reader->getUnreadLength() > 0) {
            $payloadLength = LE::readUnsignedInt($reader);
            $payload = $reader->readByteArray($payloadLength);
            $this->handlePacket($payload);
        }
    }

    private function handlePacket(string $payload): void {
        if ($this->expected !== null) {
            $offset = 0;
            $packetID = Binary::readUnsignedVarInt($payload, $offset) & DataPacket::PID_MASK;
            if ($packetID === $this->expected) {
                $this->writer->write(Binary::writeInt($this->id) . $payload);
                $this->expected = match ($packetID) {
                    ProxyPacketIds::CONNECTION_REQUEST => ProtocolInfo::REQUEST_CHUNK_RADIUS_PACKET,
                    ProtocolInfo::REQUEST_CHUNK_RADIUS_PACKET => ProtocolInfo::SET_LOCAL_PLAYER_AS_INITIALIZED_PACKET,
                    ProtocolInfo::SET_LOCAL_PLAYER_AS_INITIALIZED_PACKET => null,
                };
            }
        } else {
            $this->writer->write(Binary::writeInt($this->id) . $payload);
        }
    }

    public function write(string $buffer, bool $decodeNeeded): void
    {
        $flags = 0;
        if ($decodeNeeded) {
            $flags |= Client::FLAG_PACKET_DECODE_NEEDED;
        }
        if (($compressionNeeded = strlen($buffer) > Client::COMPRESSION_THRESHOLD)) {
            $flags |= Client::FLAG_PACKET_COMPRESSED;
            $payload = @snappy_compress($buffer);
        }
        
        $payload = $compressionNeeded ? @snappy_compress($buffer) : $buffer;
        $payload = Binary::writeInt(strlen($payload) + 1) .
            Binary::writeByte($flags) .
            $payload;
        
        $dataLength = strlen($payload);
        $totalSent = 0;
        $writeAttempts = 0;

        while ($totalSent < $dataLength) {
            $sent = @socket_write($this->socket, substr($payload, $totalSent));
            if ($sent === false) {
                $this->logger->debug("failed to write data to socket: " . socket_strerror(socket_last_error($this->socket)));
                $this->close();
                return;
            }
            if ($sent === 0) {
                $writeAttempts++;
                if ($writeAttempts > 10) {
                    $this->logger->debug("socket_write wrote 0 bytes for 10 times, closing connection");
                    $this->close();
                    return;
                }
            }
            $totalSent += $sent;
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->buffer = "";
        socket_close($this->socket);
        $this->logger->debug("Closed client " . $this->id);
    }

    public function __destruct()
    {
        $this->logger->debug("Garbage collected client " . $this->id);
    }
}
