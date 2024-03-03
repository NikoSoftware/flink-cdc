package org.apache.flink.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterModel {

    public String name;

    public Integer tem;

    public Integer hum;



}
