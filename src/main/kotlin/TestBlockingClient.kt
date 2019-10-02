
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.lang.Double.max
import java.lang.Double.min
import java.lang.Integer.parseInt
import java.net.InetSocketAddress
import java.net.Socket
import java.util.*
import java.util.concurrent.TimeUnit.NANOSECONDS
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration

private val RESPONSE_PATTERN = """ID: (\d+) *""".toRegex()

@ExperimentalTime
fun client() {

    val socket = Socket()
    socket.connect(InetSocketAddress("localhost", 2345))

    val outs = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
    val ins = DataInputStream(BufferedInputStream(socket.getInputStream()))

    val start = System.nanoTime()

    val random = Random(100) // Create repeatable random numbers

    for (idx in 0..100000) {

        val rand = min(max(random.nextGaussian(), -1.0), 1.0)

        val len = ((rand * 2000) + 3000).toInt()
        val payload = " ".repeat(((rand * 100) + 200).toInt())

        val requestBytes = "ID: $idx, LEN: $len $payload".toByteArray(Charsets.UTF_8)
        outs.writeInt(requestBytes.size)
        outs.write(requestBytes)
        outs.flush()

        val responseLen = ins.readInt()
        val response = String(ins.readNBytes(responseLen), Charsets.UTF_8)
        val match = RESPONSE_PATTERN.matchEntire(response) ?: throw IllegalArgumentException("Invalid Response")

        val (idStr) = match.destructured
        val id = parseInt(idStr)

        require(id == idx) { "Invalid Response" }
    }

    val end = System.nanoTime()

    val total = (end - start).toDuration(NANOSECONDS)
    print("Clock: $total")
}

@ExperimentalTime
fun main() {
    client()
}
