
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelDuplexHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.util.ReferenceCountUtil.release
import java.lang.Integer.parseInt


val REQUEST_PATTERN = """ID: (\d+), LEN: (\d+) *""".toRegex()

class RequestHandler : ChannelDuplexHandler() {

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        val buf = msg as ByteBuf
        try {

            val request = buf.readCharSequence(buf.readableBytes(), Charsets.UTF_8)
            val match = REQUEST_PATTERN.matchEntire(request) ?: throw IllegalArgumentException("Invalid Request")

            val (idStr, lenStr) = match.destructured
            val id = parseInt(idStr)
            val len = parseInt(lenStr)

            val response = "ID: $id ${" ".repeat(len)}"

            val responseBuf = ctx.alloc().buffer(response.length)
            responseBuf.writeCharSequence(response, Charsets.UTF_8)

            ctx.writeAndFlush(responseBuf)
        }
        finally {
            release(buf)
        }
    }

}


fun main() {

    val bossGroup = NioEventLoopGroup()
    val workerGroup = NioEventLoopGroup()

    try {
        ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(
                object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(
                            LengthFieldPrepender(4),
                            LengthFieldBasedFrameDecoder(Int.MAX_VALUE, 0, 4, 0, 4),
                            RequestHandler()
                        )
                    }
                }
            )
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .bind(2345)
            .syncUninterruptibly()
            .channel()
            .closeFuture()
            .sync()

    } finally {
        bossGroup.shutdownGracefully()
        workerGroup.shutdownGracefully()
    }

}
