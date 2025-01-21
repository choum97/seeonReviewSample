package com.seeon.naverBlog;

public enum TargetErrorCode {
    GPT("E0001"), //GTP 에러
    NAVER("E0002"),
    ETC ("E0003"),
    STOP_GPT("E0004")// gpt 응답에러
    ;
    private String value;

    private TargetErrorCode(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

}
