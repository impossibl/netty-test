import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import java.lang.Double.max
import java.lang.Double.min
import java.lang.Integer.parseInt
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit.NANOSECONDS
import kotlin.time.ExperimentalTime
import kotlin.time.toDuration

class RequestEncoder: ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {

        val req = msg as Request

        val buf = ctx.alloc().buffer(200)
        buf.writeCharSequence("ID: ${req.id}, LEN: ${req.len} ${req.payload}", Charsets.UTF_8)

        super.write(ctx, buf, promise)
    }

}

private val RESPONSE_PATTERN = """ID: (\d+) *""".toRegex()

class ResponseHandler: ChannelDuplexHandler() {

    private val requests = ConcurrentHashMap<Int, CompletableFuture<Void>>()

    override fun write(ctx: ChannelHandlerContext?, msg: Any, promise: ChannelPromise?) {

        // Save request completion handles
        val request = msg as Request
        requests[request.id] = request.handle

        super.write(ctx, msg, promise)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        val resBuf = msg as ByteBuf
        val res = resBuf.readCharSequence(resBuf.readableBytes(), Charsets.UTF_8)
        val match = RESPONSE_PATTERN.matchEntire(res) ?: throw IllegalArgumentException("Invalid Response")

        val (idStr) = match.destructured
        val id = parseInt(idStr)

        // Notify writer of response
        requests[id]?.complete(null)

        super.channelRead(ctx, msg)
    }

}

data class Request(
    val id: Int,
    val len: Int,
    val payload: String,
    val handle: CompletableFuture<Void>
)

@ExperimentalTime
fun client(bootstrap: Bootstrap) {

    val start = System.nanoTime()

    val random = Random(100) // Create repeatable random numbers

    val channel = bootstrap.connect("localhost", 2345).syncUninterruptibly().channel()

    for (idx in 0..100000) {

        val rand = min(max(random.nextGaussian(), -1.0), 1.0)

        val len = ((rand * 2000) + 3000).toInt()
        val payload = " ".repeat(((rand * 100) + 200).toInt())
        val handle = CompletableFuture<Void>()

        channel.writeAndFlush(Request(idx, len, payload, handle))

        handle.get()
    }

    val end = System.nanoTime()

    val total = (end - start).toDuration(NANOSECONDS)
    print("Clock: $total")
}

@ExperimentalTime
fun main() {

    val group = NioEventLoopGroup()

    try {

        val clientBootstrap = Bootstrap()
            .group(group)
            .channel(NioSocketChannel::class.java)
            .handler(
                object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(
                            LengthFieldPrepender(4),
                            RequestEncoder(),
                            LengthFieldBasedFrameDecoder(Int.MAX_VALUE, 0, 4, 0, 4),
                            ResponseHandler()
                        )
                    }
                }
            )

        client(clientBootstrap)

    } finally {
        group.shutdownGracefully()
    }

}
