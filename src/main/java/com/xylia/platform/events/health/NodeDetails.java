package com.xylia.platform.events.health;


import lombok.*;

@Builder
@AllArgsConstructor
@ToString
@Getter
@Setter
public class NodeDetails {

    private int id;
    private String host;
    private int port;
    private String rack;
}
