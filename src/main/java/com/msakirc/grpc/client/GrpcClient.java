package com.msakirc.grpc.client;

import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.msakirc.grpc.server.data.Cube;
import com.msakirc.grpc.server.grpc.GsonBytesCubeService;
import com.msakirc.grpc.server.grpc.GsonBytesCubeService.CubeRequestGson;
import com.msakirc.grpc.server.grpc.GsonBytesCubeService.OlapCubeGsonByte;
import com.msakirc.grpc.server.proto.CubeRequestJackson;
import com.msakirc.grpc.server.proto.CubeRequestJson;
import com.msakirc.grpc.server.proto.CubeRequestProto;
import com.msakirc.grpc.server.proto.CubeShareServiceJacksonGrpc;
import com.msakirc.grpc.server.proto.CubeShareServiceJacksonGrpc.CubeShareServiceJacksonBlockingStub;
import com.msakirc.grpc.server.proto.CubeShareServiceJsonGrpc;
import com.msakirc.grpc.server.proto.CubeShareServiceJsonGrpc.CubeShareServiceJsonBlockingStub;
import com.msakirc.grpc.server.proto.CubeShareServiceProtoGrpc;
import com.msakirc.grpc.server.proto.OlapCubeJackson;
import com.msakirc.grpc.server.proto.OlapCubeJson;
import com.msakirc.grpc.server.proto.OlapCubeProto;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import javax.annotation.PostConstruct;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GrpcClient {
  
  private ManagedChannel channel;
  private Random random;
  private Marshaller<Cube> cubeMarshaller;
  private ProtobufMapper mapper;
  private ProtobufSchema schemaWrapper;
  
  @PostConstruct
  private void helloClient () throws IOException, ExecutionException, InterruptedException {
    
    channel = ManagedChannelBuilder.forAddress( "localhost", 8080 )
                                   .usePlaintext()
                                   .build();
    
    random = new Random();
    cubeMarshaller = GsonBytesCubeService.marshallerFor( Cube.class );
    
    mapper = new ProtobufMapper();
    schemaWrapper = mapper.generateSchemaFor( Cube.class );
  }
  
  private int getUserId () {
    return random.nextInt( 20 );
  }
  
  private int getCubeId () {
    return random.nextInt( 11000 );
  }
  
  @GetMapping( "/proto" )
  private void runProtoBufClient () {
    CubeShareServiceProtoGrpc.CubeShareServiceProtoBlockingStub protoStub = CubeShareServiceProtoGrpc.newBlockingStub( channel );
    
    OlapCubeProto mondrianCube = protoStub.getMondrianCubeProto( CubeRequestProto.newBuilder()
                                                                                 .setCubeId( String.valueOf( getCubeId() ) )
                                                                                 .setUserId( String.valueOf( getUserId() ) )
                                                                                 .build() );
    
    System.out.println( mondrianCube );
    // channel.shutdown();
  }
  
  @GetMapping( "/jackson" )
  private void runJacksonByteClient () throws IOException {
    CubeShareServiceJacksonBlockingStub jacksonStub = CubeShareServiceJacksonGrpc.newBlockingStub( channel );
    
    OlapCubeJackson olapCubeJackson = jacksonStub.getOlapCubeJackson( CubeRequestJackson.newBuilder()
                                                                                        .setCubeId( String.valueOf( getCubeId() ) )
                                                                                        .setUserId( String.valueOf( getUserId() ) )
                                                                                        .build() );
    
    byte[] protobufData = olapCubeJackson.getCube().toByteArray();
    Cube cube = mapper.readerFor( Cube.class )
                      .with( schemaWrapper )
                      .readValue( protobufData );
    
    System.out.println( cube );
    // channel.shutdown();
  }
  
  @GetMapping( "/gsonbyte" )
  private void runGsonByteClient () throws ExecutionException, InterruptedException {
    
    ClientCall<CubeRequestGson, OlapCubeGsonByte> call = channel.newCall( GsonBytesCubeService.SHARE_SERVICE, CallOptions.DEFAULT );
    CubeRequestGson cubeRequestGson = new CubeRequestGson();
    cubeRequestGson.cube_id = String.valueOf( getCubeId() );
    cubeRequestGson.user_id = String.valueOf( getUserId() );
    
    ListenableFuture<OlapCubeGsonByte> res = ClientCalls.futureUnaryCall( call, cubeRequestGson );
    
    Cube receivedCube = cubeMarshaller.parse( new ByteArrayInputStream( res.get().value ) );
    
    System.out.println( receivedCube );
    // channel.shutdown();
  }
  
  @GetMapping( "/gsonjson" )
  private void runGsonJsonClient () {
    CubeShareServiceJsonBlockingStub jsonStub = CubeShareServiceJsonGrpc.newBlockingStub( channel );
    
    OlapCubeJson olapCubeJson = jsonStub.getOlapCubeJson( CubeRequestJson.newBuilder()
                                                                         .setCubeId( String.valueOf( getCubeId() ) )
                                                                         .setUserId( String.valueOf( getUserId() ) )
                                                                         .build() );
    
    Cube cube = new Gson().fromJson( olapCubeJson.getCube(), Cube.class );
    System.out.println( cube );
    // channel.shutdown();
  }
}
