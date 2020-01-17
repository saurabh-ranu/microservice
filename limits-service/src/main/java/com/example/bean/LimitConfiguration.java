package com.example.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class LimitConfiguration {

	int maximum;
	int minimum;
	
}
