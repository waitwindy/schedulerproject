
package com.ultrapower.scheduler.init.impl;


/**
 * @author wqf
   @desc :初始化基类
 * @date:2015-8-20 上午10:52:54
 * @version :4.0
 */
public interface InitInterface {

	/**
	 * 
	 * @author wqf
	   @desc :启动方法 会在init方法后执行
	 * @date:2015-8-20 上午10:53:33
	 * @version :4.0
	 */
	public void start();
	
    /**
     * 
     * @author wqf
       @desc : 初始化方法 会第一个执行
     * @date:2015-8-20 上午10:55:52
     * @version :4.0
     * @return
     */
	public boolean  init();
	
	public boolean close(); 
	
	public boolean checkInit();

}
