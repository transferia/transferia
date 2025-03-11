package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	"github.com/transferria/transferria/pkg/debezium/testutil"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source = helpers.RecipeMysqlSource()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Source.AllowDecimalAsFloat = true
}

//---------------------------------------------------------------------------------------------------------------------

// fill 't' by giant random string
var update1Stmt = `UPDATE customers3 SET text_ = 'LidVY09K[5iKehWaIO^A7W;_jaMN^ij\\aUJb^eQdc1^XT?=F3NN[YBZO_=B]\u003c4SaNJTHkL@1?6YcDf\u003eHI[862bUb4gT@k\u003c6NUZfU;;WJ@EBU@P2X@9_B0I94F\\DEhJcS9^=Did^\u003e\u003e4cMTd;d2j;3HD7]6K83ekV2^cF[\\8ii=aKaZVZ\\Ue_1?e_DEfG?f2AYeWIU_GS1\u003c4bfZQWCLKEZE84Z3KiiM@WGf51[LU\\XYTSG:?[VZ4E4\u003cI_@d]\u003eF1e]hj_XJII862[N\u003cj=bYA\u003c]NUQ]NCkeDeWAcKiCcGKjI:LU9YKbkWTMA:?_M?Yb9E816DXM_Vgi7P7a1jXSBi]R^@aL6ja\u003e0UDDBb8h]65C\u003efC\u003c[02jRT]bJ\u003ehI4;IYO]0Ffi812K?h^LX_@Z^bCOY]]V;aaTOFFO\\ALdBODQL729fBcY9;=bhjM8C\\CY7bJHCCZbW@C^BKYTCG]NTTKS6SHJD[8KSQcfdR]Pb5C9P2]cIOE28U\u003eH2X\\]_\u003cEE3@?U2_L67UV8FNQecS2Y=@6\u003ehb1\\3F66UE[W9\u003c]?HH\u003cfi5^Q7L]GR1DI15LG;R1PBXYNKhCcEO^CTRd[3V7UVK3XPO4[55@G]ie=f=5@\\cSEJL5M7\u003c7]X:J=YMh^R=;D;5Q7BUG3NjHhKMJRYQDF\\]SJ?O=a]H:hL[4^EJacJ\u003ee[?KIa__QQGkf=WXUaU6PXdf8[^QiSKXbf6WZe\u003e@A\u003e5\u003cK\\d4QM:7:41B^_c\\FCI=\u003eOehJ7=[EBg3_dTB4[L7\\^ePVVfi48\u003cT2939F]OWYDZM=C_@2@H^2BCYh=W2FcVG1XPFJ428G\\UT4Ie6YBd[T\u003cIQI4S_g\u003e;gf[BF_EN\u003c68:QZ@?09jTEG:^K]QG0\\DfMVAAk_L6gA@M0P\\1YZU37_aRRGiR9BMUh^fgRG2NXBkYb[YPKCSQ8I8Y6@hH]SEPMA7eCURUT@LEi1_ASEI1M7aTG^19FEZcVa]iJDS4S4HR4\u003ccXRAY4HNX_BXiX3XPYMAWhU?0\u003eBH_GUW3;h\\?F?g:QT8=W]DB3k?X??fQWZgAGjLD[[ZjWdP@1]faO@8R?G@NV;4Be0SAk4U[_CZK\u003c\u003e[=0W3Of;6;RFY=Q\\OK\\7[\\\u003cELkX:KeI;7Ib:h]E4hgJU9jFXJ8_:djODj\u003cOK6gV=EMGC?\\F\u003cXaa_\u003cM?DAI=@hQ@95Z?2ELGbcZ6T5AAe77ZCThWeFd;CJJMO9\\QN=hE5WKY\\\\jVc6E;ZBbTX\\_1;\u003eMZG\u003e@eK=?PdZ=UK=@CBUO2gFVU7JUBW713EAiO=DHgR2G^B[6g\u003e7cU]M[\u003c72c\u003e3gSEdHc6\\@2CBI7T9=OGDG16d\\Bk^:\u003ea5a;j\u003e35jC6CUPI=XV]4j9552aG2TQ@JV6UUDXZD0VUE5b2[T6Z];_1;bU\\75H=Z2QG\\eGQP1eUdgEM34?\u003ec4?4fd2i=?W?a3j[JP@LJeDG?aIC6W\u003c:f?5_47]AFIP;LOff3;GN5[dDRBXXicad8fX\u003c1JMGc2RDPM?TXV6]Gj6hB^U@VK:^FbkGAM^9OFM4c\\XPG^B]^H[5;DEa_OU:FTQW6E_U[AYS2G8H:J:hbe22\u003eGd3eM=@7^g=8[bc1PK2gRK61U3cO4e]K^E@2UGPTh@KA0?Cgb^2cH5[g9VYTINiYPS5D8YAH96Y:F26\u003c84==_9FJbjbEhQeOV\u003eWDP4MV^W1_]=TeAa66jLObKG\u003cHg6gRDTfdXHOK4P?]cZ3Z9YBXO]4[:1a7S;ZN4HfSbj87_djNhYC5GU]fGaVQbMXJWGh[_cCVbJ]VD\\9@ILE68[MiF3c[?O8\u003c?f4RRf1CPE4YUN:jCA73^5IaeAR9YE5TIV;CWNd1RRV5]UH2[JcWZ9=cjf=3PVZ[jF\u003ebGaJ2f;VB\u003eG\\3\u003cUZf^g^]bkGVO7TeELB:eD56jGDF8GQ]5LP1?Bc?8?dWENQZjcdd\u003cij;ECQMY7@_Sb7X6?fjf@MLjKDcEPaD[;V@XEHh8k]hbdUg8Pf2aHOccX=HNQ7Y\u003cHFQ_CY_5VVi@R5M8VeVK^N8kfVQ2E]J[B\u003e3038WY6g@;\\]CGXibKLjKFU0Hj]bZ46]48e[akW6:HcMPKW0gUKB@KZ\u003e=QhAWZF_T6US][^;T@j9[V9VAUhP5W_B=\\TdKjX45BWb3J2VZ1JWi5hS2MXYAjg1SLQMPV_\u003cMbUOMDPB^=@c:ceWOThNOi6DJWajBU:_L_Cj9cAg5Q_?IYehBbKaQ:?\u003ek\u003ePUHD6\u003cW5EOFATg5bE^]B5T]fID5XQ4f6ZBJO6ecUA9\u003e=\u003e5R0bc5KVkdi4QP9KVb^5WA;R:_bC24P7UQiNVI8UB7ZcVbCAY6FFGQgQE^dGbINLjMjUf7?=\u003ei5dI:OOQef6aLLTEcK^Fg]cfG^2W0?U59JNCi2dchjXIJA^B\\QYXCQSZDTFDd0J1JhDIi=@f\u003ciDV?6i0WVXj\u003c@ZPd5d\\5B]O?7h=C=8O:L:IR8I\u003e^6\u003ejFgN?1G05Y^ThdQ:=^B\\h^fGE3Taga_A]CP^ZPcHCLE\u003c2OHa9]T49i7iRheH\\;:4[h^@:SAO_D3=9eFfNJ4LQ23MgK\u003e7UBbR58G?[X_O1b\\:[65\u003eP9Z6\u003c]S8=a\u003eb96I==_LhM@LN7=XbC]5cfi7RQ\u003e^GMUPS2]b\u003e]DN?aUKNL^@RV\u003cFTBh:Q[Q3E5VHbK?5=RTKI\u003eggZZ\u003cAEGWiZT8@EYCZ^h6UHE[UgC5EQ1@@ZLQ5d=3Sa;b;c:eV80AOE09AD\u003eVd?f9iGZ3@g5b^@Zi9db_0b5P\u003c5YMHg8B:3K8J:;Z6@QdP@bY9YM:PRY]WG?4CGFMJaVd0S76:kVJbDSPa]5HKb3c67;MMXgCCaC8IJ\u003eSJd2@=U3GeKc\\NZaUeD7R@Kd6^1P=?8V8:fE[H\u003cUb4EE^\u003ckWO7\u003eR8fD9JQHR\u003cP\\7eQbA]L8aaNS2M@QTNF;V@O_[5\u003cBA\\3IVT@gG\\4\u003cRRS459YROd=_H1OM=a_hd\u003cSMLOd=S6^:eG\u003ejPgQ4_^d\u003c_GZ1=Ni6ZQT;5MHXR;aMR4K7k2;_31TK[UX=S^h9G8\u003ecPfK[\\gAHHJST?WUc7EM_R6RO?iWMa;HAf9==jUU_4=IBd3;jHX^j^EN2C:O9EhJ@6WL5A6dECBW\u003cDa;\\Ni[AC\u003eCVGc_\\_=1eeMj;TcOg:;8N1C?PAjaT=9\u003eT12E?FZ9cYCLQbH[2O\u003e4bMT8LJ[XSiAT0VI?18Hdb\\EHS]8UAFY8cB@C[k1CiBgihE\u003ehMVaDF\u003c\\iidT??BG6TWJDWJWU\\TSXiaVKLL_bXPVIIeX[A^Ch=WTWD\u003eHga5eW[E8\u003c9jdYO7\u003eH^iYQAV^i?JAMb=Dg7kWL8dU7]CgAI9Y=7G^H3PFBjW_ad7\\17IM?A7F3JBDcK25RIbjLHE^G0Q\u003ceXie_FG3WNJZh[3;5e^O\\]k96]O7C\\00Yf5Bc\\BK]2NR\u003eTK07=]7Ecdej\u003cUj\u003cDe1H\u003ce91;U^=8DK\\Kc1=jG5b@43f3@?hAW9;:FJgSRA3C6O;7\\9Na1^d4YgDgdUS2_I\u003c:c8^JIa]NEgU558f6f:S\\MPU78WfPc5HkcbHYSf3OP8UX3[Scd;TG[\u003eNcfIH]N]FW:4?57_U?HCB8e:16^Ha2eYhC6ZagL\u003cSV@b[GVEU3Xh;R7\u003cXeTNgN\u003cdaBSW=3dY9WIOB^:EK6P2=\\Z7E=3cIgYZOFhR\u003e]@GIYf[L55g\u003cUiIFXP[eTSCPA23WjUf\\eB:S=f3BkjNUhgjULZN5BaTScX?bB:S\u003cK^_XXbkXaNB^JAHfkfjA\\SdT@8KRB3^]aRJNIJ;@hL3F]JA]E@46chZ85:ZG\u003eM934TQN3\\]k=Fk?W]Tg[_]JhcUW?b9He\u003e1L[3\u003cM3JBIIQ5;:11e^D]UiIdRAZA;PEG2HaD@feK5fKj[\u003eCLdAe]6L2AD0aYHc5\u003e=fM7h\u003cZI;JWOfPAfAD[QX[GE8?JFLEcS9_d\u003ejBeN=JB2[=B4hd[X@5_OP:jd2R3bFf5E=kbKI:L9F_=CXijg3_KSiJL01ObGJh\\WgS7F]TO8G\\K4ZJ0]\u003eKE\u003cea\u003cfE3B_03KgVRBG;aORRjVAIV3W6Hc0=4gR7\u003eF7Aa3fHECR;b9]a_3?K5eQM]Q[aMBh[W40M7feM\u003eLW5VIfJL:eQ4K3a1^WN5T=\\X=\u003e_98AGUhM?FHYbRSIV3LL4?8RD\\_5H1C\u003c:LMQ5J3DaK3X1V6WYR8]a@D:17?I9SVC38d8RgLHGO5H:;4c]=USMi]N52g\u003eTQQWYJ_@FAX\\]9jh\u003ebZKLBhJ4JO6F]ZhBFV\\;f6KSc@F1?B?61ZSCW1H6PNLB=ITS4E^jK\u003eSCOhD^@SdABLTiM142NPD[igD2A71\\ET4dQGWajP7A0[?M\\CO?ccja_Cc5Jda_NeX4ACeAc1Rc\\aFM9e\\1][bR3ZWMTM@6Gh:X@4i85P1aGGBPA3Q3^HUa7ABZ^Sa:Pkb4h8Fii\\E@AUCbX6\u003eBgES\u003e5EaeOFeG:i\u003c86R54CJDT4XJ]^Y4Z3Vi80_2P9ggDe8KjZQ32kHU444b]dROOhPCj4Lf0_8@_bbd?NdCRY;DR\\96@5VS4Z4jZc^c8QZhHR]W5VkWD:0fg91\u003c?V_CEcA5[4gcVVa3=SZB=ZiQeiL7M1F8XMXjRI3NAX97[EZKWg:UM3RidYKe4SZ]6H[Xa^;7KC=\u003cYgVEcjFcQD\\?_VDGE5M]:SSDY4Xg@Fcf[[[Y6T?JDO\u003ejbUEg77]AYEUGIBCXX;SGfC50gDJ@cX@ZBTVI[HZI]D;V8cCCLZ=__\u003e[9X01E@[WeF5T_2Q9c\\kT7B5bPdV^T_JT__dOK^eQGYEJ?OAjCASKSXA8Qgf9[E^O9W3UJh:aVP@e3QdGbMaK:8S[4Nd^cVB1BEV\\BSiEbcHI\\_@\u003eU[H]C70SXWeYi?DZQ9BON9GfR8YbFCR^5eeeZfNGQH5OWI?\u003eRQ]5Z9jA@Y9V1ZI6TDkC\u003eNZ_f_DR\u003eS8QecZd9jRAVS14YUHYhV;WJ6K^XYFLNN2HF\\BO[dFLaJ9KbbHL24g8OZ=4A[SC8h4JLCA;^7UhRL_jha3diRR^_W3O\u003eFW\u003cJ6X?IiJ\u003c549XOhWM^ZE\\@hO4TRSbh?3GE[V]Y5i^97KY47:baOS6L7:5X\\gUkj1DZX7H]5;f\u003cWT@^^8SB[Y_acdNT8T_:iNb4eT:6OF]8VOf^8=Ma1CYdbBYjgM9ejkieS8k8M\\@9@;gHHI\u003eI]gBS\u003e0R:M[4L[2FC9EKW6[Ge[_B91[fh2N;36EPaI1QKGdT\\D?b34\u003eh_2@i3kd02G\u003c5MQUCjUcI1\\2]4BT8Ec5:eD7hDkhFG9KdZ5;YZ38[_:MdK70aj5jcJ7^6]:MfUFUZQDIUK:IUWB5^Bf]HfUb1JU8\u003c^U7Hk]7Q6P:QZS;Ge@:\u003c\u003cfT6PK7j4?;cdC@c5GI:gS[W\u003cf26;\u003cBG7fMXFTWJcbB\\9QT\u003eh3HdV8Pb3Rh\u003e^?Ue:7RP[=jT4AE\u003ebiL_1dYW1\u003eM4JCSYhMc44H_AGHEX]SO[3C[g1Gi?e24DDV2A8dE\u003cA9LXQbECIc2M\u003c^I\u003c:GK4IOG]:I3BCHNTQjA7aUJ?NL\\Y?:fIPFMied[4B^FU;c\u003e\\bNcX9AgW]WE1a@JFVgDPa4S8bi]2ak]XNUEWfACXhXY^h9:S5N8eR[2IY_JO_==BbRi]cAJh8TeA^MFAU@cEB@36[Reh_\u003c_F9P\u003eJj3G8WAHJ_^ZH3R]EbKRGEO;PCPZc^9baPjMaHfU;V2\u003e=R4U3W1G;\u003chN\\WFO_=DD\u003ca:T]_^Gb1TVSX@VDA2OMj2=VG\\JU6^agiJY]=5T\u003eY?bFOMZO\u003eBO@O:W@TAFG7BEQj7^4[1]jc9NEcCd7UHG9Q3J:DQK6f162_:]ag\\Y5?3iRg4\u003cDKEeN_4bSUBZPC_R8iCie4WkCZhdV15iLJcj\u003efaaP8P4KDVSCiQ=2\u003c=Ef:\u003eP\u003cDNX^FW1AMcaVHe6\\PY4N?AQKNeFX9fcLIP?_\u003c@5Z8fDPJAE8DcGUIb8C\u003c_L7XhP=\u003cDILI8TDL99fIN3^FIH_@P8LDSS1Q8\u003e]LW\u003ee^b\u003e?0G9Ie\u003c\u003c@UT4e9\u003cGM_jME7[6TFEN:\u003c\\H\u003c8RU2]aBHJFBSRY5FXR[_BbHY;ebGV?S^a=S470NNB650;KX]\u003cL42d\\\u003e^SUJc==XJ3AN:A1XS7]TB=A3I]7KVcYJLCcCO61j8AMCRNk:U\\^gi4kGa7bMjPfKc_^Ge^F25cEWFDa06Tg4XgKN3Ck2cfMZZ?6S3LU8Cj^YCTYI=UMeQhHT?HV7C7a1GgUJH?Q[\u003eEJQi8j;]L5CILgXdR_\u003cYU=5RbOj65ZEJ9fGAeR3FWF_8CL1e@=SfJXLA\u003cKHA:\\[CW7SRYVhE1[MD\u003cN=M[G:NdKZDckNTZAaIbP4_d5OFI\\cV=SLT]iM=Xa5XCZG8k\u003eQb]UVVZ:18fe_8M?\\?\u003e\u003eLf4QSG@jO@\u003c57iZ]UIgVRaOEi1UZ@ch\\]1BEHSDgcP1iN\\[8:W^\\NB6LCZ;SR9CD:VYR=2N5RO35@_=JKk;iA@ITkU\u003cR]Ofg:TNGW0L\u003ePOC_CP\u003e^PI[aZ:KY^V@Q;;ME_k\\K0\u003eYP]1D5QSc51SfZ]FIP1Y6\u003cdRQXRC8RP7BaKGG2?L3bG]S];8_d\u003e0]RJGeQiJG5\\=O8TRG5U\u003eLGa\u003eRi2K\u003c3=1TVHN=FhTJYajbIP\u003eN:LjQB=9@@TLBaLfLdIY?FBY57XfQ\u003e93HU2ig?7\u003cO[WaP9]12;ZAQ1kV8XQYeZ\\BD_@@3GLR78HWA:YCEHTfITQQ@7?;b1M;_]Kc9gJ@4bgD1UWF2@AKdb29iADBak6SKi\\FG1J\u003eh^?RKUT[e4T\\6]ZG6OXgN_Oi\\@D8A^G\u003eQVa1?J\\:NDfT7U0=9Y9WLYU=iiF?\\]MBGCCW]3@H[eNEe[MSe94R^AP\\W_MHB_U7LG:AWR1Q5FKc2Z16A_GaQ3U2Kga@Qh\\h71TY29]HTS@VBA\\S68IV;4YVkOfQLVMSX6AZ?37cVFNgX?O]GhIQ16\u003c1U7Q6]3ZI9j8H2?@XU^TB284I6Mj7S;7=BYD4\\3Me2UC4dS\\NFEIMdbSFaZi1a\u003cCOPG@Re;TOMXH5IfK^[d@U[ckQRiRH:fgZB\u003cA\u003cGe[dR8ik3J]^C3H2fHSMF;eP6b?H3PSJICC0JAkMZ]@2X5[5X=Lc71hi@E1iK\u003e@^\u003e[4\u003e=^kM;eO@R\\\\Id]Gb2\\cbYC5j5CZ9QggPI\\ETVde\u003cUVVNH2EJ^=ALOFKUX:^\u003e5Z^NK88511BWWh:4iNN\\[_=?:XdbaW5fEcJ0Rf2S\u003cX?9bC7Ebc5V5E]\u003eWSe]N?Uh4UOjW7;DED;YKPODU:Hjj:=V]7H@F2=JW\\ICcTX=hbfHGJ\\2T91SC\u003e\u003e5EVE[XS:DDRX;;DH8;CPS\\ATEJUh]c;b=a=gN_6b8XOCcc[k33PV_?:?d71\\Bdi85eVdkM1X0DQc5Pf85Qge6:Y\u003c;JN3GV8A@2A]3i]GOUL4PS:6O4eU=SaH1DKIjTZ?U01Xi^4MHPRh8[3W_hA2P7JQKejJNYY8YZaWNe:fJ[cRLf?@cPBHW[i7VhQ9V?ACi7kL19GKe?3E:AU2agJMWHTBD:KjI\\CHcBddL@DEOF[YXE[NA:0hQT?f_Ze=K=UBON;j]OEAf4jRIZ5Zc5WJZfENU?[5KEGjbRjT6Ce1HdSaSYPK^\u003ceM8?j]NZai4\u003ehfgOf?JgWCPMe=2E0??MFNL81;ij?\u003cg:1cYg78d^KH?EVB[VPj8gMT4N_2M3\u003eI=?@f\u003cG349NMId8[T^@Sf\u003c5O?SCB5FPNS_^Ok:R4C6Q\\iXLRK\\:Eg@d\u003cc\u003cMhS3K;b\u003eZbHAf[GKME9igTY7iVFba\u003e4D;WFVb=dQ4Abj2\u003eJNSSLP;:V:11V?5jK\\E6SRj8V@kUB=4aaVBEbL11A22gA6f\\b@bJbaRM7R7I_;?UaPjX1kXB2Z\u003eC94WIf6@]X]c?dA24PWe5VR6V?HWiVj__3K=iQM[\u003e@TM9eO\u003cJ;6OaXVLg38eZ7XN:8[8Y=cgMLIVFhb8hEjTjJP3RJ\\Y7?c?k0h=deZECE[@;PH8eG]daBgI[X6bhi6gj49bhc\u003c@=gPHLhQFDC@:T\u003cREdY\u003caWB]VFgMC_YS1U7J64jMHB\\Rfh9@abLWN^I99EVL9E4:j;S5?SRWeC=?F55=Q\\\\D:eMNPiWe1ad\u003cIiK1O7fbD[7[\u003chEhYY6S;T88@2:6eFOcaPGiK?B;E1kQiENW3T?\u003e=FFMHPSBf8:\\XRZ91D:2D[1Y\u003eX\\bfj4BEQZe:1A\u003cQj^@7SAK]C_NCM\\0\u003eSf=V=Q=gKFi@W:aVg6]OF=BY1_1NP2[8hh^:Nk6iF4\u003e2\u003e4X:9JYPXk\u003eX_?;DAfL\u003ec?HF\u003eNETRSWWDj^XEKXR8LaC7?@E7O\\M]@bGbJ2W6FVf:C?U0b]LX6@_EP9K4ehb:_\u003e1\u003e@XDWD?WNJWE=82CHaWhj82d5d2d648F\\K25Zb\\=BHROPTbhJNeHVgA[_CTfG\\A8\u003cC=f:i8LFZ0fCbc]D]:jYKZM_CH;3YC@1O;\u003cMCXc2X^EOV7cHAb6\\QTPc1ZgZ2;\\RFh4YUg[BZ5aE\u003cY^MPd\u003e6M^iNNe=P6i6Lf::P6ebjX;\u003cFhYfag1CZka=e3]k1cLg2VL8PCiPj9[E6IAgEB@4B6A\u003c93\u003c:fX5iCQ6cd4Hc=8=CQN?fOk6TAB]DNg@:1\u003eMRDEKH]CUePgK3;FcZFiDW@61^1@h2NJTb_4?QGcKggk0BcZXa3D69Ed:Ua\u003c8@j5e\u003eVA76=g2=gD4V1eYF0bZd0EZ\u003cMk2M4g[Z=baJ]cVY\u003c[D=U2RUdBNdW=69=8UB4E1@\u003cbZiYEWe507Y3YCfkaV4f_A2IR6_TFkJ5i9JU2OV9=XbPTaFILJC@[FZBLMfbMEgKNF6Pe[Y7IOW2F3JbM^7=8aOTCJK_G@A]FaV6O]O4JPIMk@i]H;f\u003eZOQ8jFgEV=703^6RPUVj:4K:DJg\\UbjDEOLDeHZOUaPXSV@8@f7JjSTC2P4WG3j\\RK5Lc_0MUP:=;JFJDMdC5MV72[]I]\\;D\u003c@44QYE[fO:AjN^cbcEMjH=\\ajM1CZA8^EhD3B4ia\u003e?\\2XSf25dJAU@@7ASaQ\\TfYghk0fa\u003e:Vj=BR7EW0_hV4=]DaSeQ\u003c?8]?9X4GbZF41h;FS\u003c9Pa=^SQT\u003cL:GAIP3XX[\\4RKJVLFabj20Oc\u003eBK_fW?53PNSS;ABgDeG^Pc9FZ8HZW@gi[[cGkhKPK37UCJQXDgKc_T?M\\W\u003cHg9FWd\u003e4d;NHVQP@ejaQB]1;QVI3G5@_1H:XAH[:S\u003eS\u003e7NY6C@H5ASVg1ZC6i76GA^XYNbA]JNQR1?XDO5IX4\\Y^4_\\:e8KX9;XIh7hNXh]EAAJZ66_b_RfSC5MKP:@YEg7A34_[1Q5BbN2hUIGZ1ZM9EWI30E:BH\u003e67\u003eW\u003cQNZRKDH@]_j^M_AV9g4\u003chIF\u003eaSDhbj9GMdjh=F=j:\u003c^Wj3C8jGDgY;VBOS8N\\P0UNhbe:a4FT[EW2MVIaS\u003eO]caAKi\u003cNa1]WfgMiB6YW]\\9H:jjHN]@D3[BcgX\\aJI\\FfZY1HE]9N:CL:ZjgjCjZUbVJNG?h0DZZ1[8FNAcXTEbCD^BW\\1ASW[63j3bjGRZHBb]8VM[jC3C6EjcF@K20Q5jTgikNXHN:TV6F_II8P^7G9Hb;HG@G1;E0Y2HNPR7;G=R\u003cWkC\u003c^KSgbI7?aGVaRkbA2?_Raf^\u003e9DID]07\u003cS431;BaRhX:hNJj]\u003eQS9DaBY?62169=Y=AZHSPkP=9M[TLMb36kGgB4;H6\u003cN?J\u003cLZfeCKdcX2EHVbeMd0M@g^E7;KDYZ]e;M5_?iWg01DWc\u003e8]\u003eU2:HGATaUBPG\u003c\\c0aX@_D;_EOK=]Sjk=1:VGK\u003e=4P^K\\OD\\D008D\u003cgY[GfMjeM\u003cfVbB65O:UBVEai6:j6BCB=02TgOSa1_[WU2]ZRhDdRYYQ_cOf:b=Gb?0^^ST_FDK0F=Zh93\\\\OAQGLQWYhNhhAZPeNf\u003eifT:UPDYF4JdF0@;Lab9]F6ZW?QC:^A5GKZg_HBcb;\u003ebKICA@L3VQ^BG2cZ;Vj@3Jjj\u003eFA6=LD4g]G=3c@YI305cO@ONPQhNP\u003ceaB7BV;\u003eIRKK' WHERE pk=1;`

// TOASTed update
var update2Stmt = `UPDATE customers3 SET bool1=true WHERE bool1=false;`

// update with pkey change
var update3Stmt = `UPDATE customers3 SET pk=2 WHERE pk=1;`
var deleteStmt = `DELETE FROM customers3 WHERE 1=1;`
var insertStmt = `
INSERT INTO customers3 VALUES (
    1,

    0,     -- BOOLEAN
    1,     -- BOOL
    1,     -- BIT(1)
    X'9f', -- BIT(16)

    1,   -- TINYINT
    22,  -- TINYINT DEFAULT 0
    255, -- TINYINT UNSIGNED

    1,   -- TINYINT(1)
    1,   -- TINYINT(1) UNSIGNED

    1000, -- SMALLINT
    100,  -- SMALLINT(5)
    10,   -- SMALLINT UNSIGNED

    1,    -- MEDIUMINT
    11,   -- MEDIUMINT(5)
    111,  -- MEDIUMINT UNSIGNED

    9,     -- INT
    99,    -- INTEGER
    999,   -- INTEGER(5)
    9999,  -- INT UNSIGNED

    8,    -- BIGINT
    88,   -- BIGINT(5)
    888,  -- BIGINT UNSIGNED

    -- REAL

    123.45,   -- REAL
    99999.99, -- REAL(10, 2)

    1.23, -- FLOAT
    1.23, -- FLOAT(53)

    2.34, -- DOUBLE
    2.34, -- DOUBLE PRECISION

    -- CHAR

    'a',   -- CHAR
    'abc', -- CHAR(5)

    'blab', -- VARCHAR(5)

    X'9f', -- BINARY
    X'9f', -- BINARY(5)

    X'9f9f', -- VARBINARY(5)

    X'9f9f9f',     -- TINYBLOB
    'qwerty12345', -- TINYTEXT

    X'ff',               -- BLOB
    'my-text',           -- TEXT
    X'abcd',             -- MEDIUMBLOB
    'my-mediumtext',     -- MEDIUMTEXT
    X'abcd',             -- LONGBLOB
    'my-longtext',       -- LONGTEXT
    '{"k1": "v1"}',      -- JSON
    'x-small',           -- ENUM('x-small', 'small', 'medium', 'large', 'x-large')
    'a',                 -- SET('a', 'b', 'c', 'd')

    -- TEMPORAL DATA TYPES

    1901, -- YEAR
    2155, -- YEAR(4)

    '1999-01-01 00:00:01',        -- TIMESTAMP
    '1999-10-19 10:23:54',        -- TIMESTAMP(0)
    '2004-10-19 10:23:54.1',      -- TIMESTAMP(1)
    '2004-10-19 10:23:54.12',     -- TIMESTAMP(2)
    '2004-10-19 10:23:54.123',    -- TIMESTAMP(3)
    '2004-10-19 10:23:54.1234',   -- TIMESTAMP(4)
    '2004-10-19 10:23:54.12345',  -- TIMESTAMP(5)
    '2004-10-19 10:23:54.123456', -- TIMESTAMP(6)

    -- TEMPORAL TYPES

    '1000-01-01',   -- DATE

    '04:05:06',        -- TIME
    '04:05:06',        -- TIME(0)
    '04:05:06.1',      -- TIME(1)
    '04:05:06.12',     -- TIME(2)
    '04:05:06.123',    -- TIME(3)
    '04:05:06.1234',   -- TIME(4)
    '04:05:06.12345',  -- TIME(5)
    '04:05:06.123456', -- TIME(6)

    '2020-01-01 15:10:10',        -- DATETIME
    '2020-01-01 15:10:10',        -- DATETIME(0)
    '2020-01-01 15:10:10.1',      -- DATETIME(1)
    '2020-01-01 15:10:10.12',     -- DATETIME(2)
    '2020-01-01 15:10:10.123',    -- DATETIME(3)
    '2020-01-01 15:10:10.1234',   -- DATETIME(4)
    '2020-01-01 15:10:10.12345',  -- DATETIME(5)
    '2020-01-01 15:10:10.123456', -- DATETIME(6)

    -- DECIMAL TYPES

    1234567890, -- NUMERIC
    12345,      -- NUMERIC(5)
    123.45,     -- NUMERIC(5,2)

    2345678901, -- DECIMAL
    23451,      -- DECIMAL(5)
    231.45      -- DECIMAL(5,2)

    -- SPATIAL TYPES

#     ST_GeomFromText('LINESTRING(0 0,1 2,2 4)'),                                          -- LINESTRING_         GEOMETRY,
#     ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'),        -- POLYGON_            GEOMETRY,
#     ST_GeomFromText('MULTIPOINT(0 0, 15 25, 45 65)'),                                    -- MULTIPOINT_         GEOMETRY,
#     ST_GeomFromText('MULTILINESTRING((12 12, 22 22), (19 19, 32 18))'),                  -- MULTILINESTRING_    GEOMETRY,
#     ST_GeomFromText('MULTIPOLYGON(((0 0,11 0,12 11,0 9,0 0)),((3 5,7 4,4 7,7 7,3 5)))'), -- MULTIPOLYGON_       GEOMETRY,
#     ST_GeomFromText('GEOMETRYCOLLECTION(POINT(3 2),LINESTRING(0 0,1 3,2 5,3 5,4 7))')    -- GEOMETRYCOLLECTION_ GEOMETRY,
);
`

func ReadTextFiles(paths []string, out []*string) error {
	for index, path := range paths {
		valArr, err := os.ReadFile(yatest.SourcePath(path))
		if err != nil {
			return xerrors.Errorf("unable to read file %s: %w", path, err)
		}
		val := string(valArr)
		*out[index] = val
	}
	return nil
}

func TestReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------

	var canonizedDebeziumInsertKey = ``
	var canonizedDebeziumInsertVal = ``

	var canonizedDebeziumUpdate1Key = ``
	var canonizedDebeziumUpdate1Val = ``

	var canonizedDebeziumUpdate2Key = ``
	var canonizedDebeziumUpdate2Val = ``

	var canonizedDebeziumUpdate30Key = ``
	var canonizedDebeziumUpdate30Val = ``

	var canonizedDebeziumUpdate31Key = ``
	var canonizedDebeziumUpdate31Val *string = nil

	var canonizedDebeziumUpdate32Key = ``
	var canonizedDebeziumUpdate32Val = ``

	var canonizedDebeziumDelete0Key = ``
	var canonizedDebeziumDelete0Val = ``

	var canonizedDebeziumDelete1Key = ``
	var canonizedDebeziumDelete1Val *string = nil

	err := ReadTextFiles(
		[]string{
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_0_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_0_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_1_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_1_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_2_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_2_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_3_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_3_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_4_key.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_5_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_5_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_6_key.txt",
			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_6_val.txt",

			"transfer_manager/go/tests/e2e/mysql2mock/debezium/debezium_replication/testdata/debezium_msg_7_key.txt",
		},
		[]*string{
			&canonizedDebeziumInsertKey,
			&canonizedDebeziumInsertVal,

			&canonizedDebeziumUpdate1Key,
			&canonizedDebeziumUpdate1Val,

			&canonizedDebeziumUpdate2Key,
			&canonizedDebeziumUpdate2Val,

			&canonizedDebeziumUpdate30Key,
			&canonizedDebeziumUpdate30Val,

			&canonizedDebeziumUpdate31Key,

			&canonizedDebeziumUpdate32Key,
			&canonizedDebeziumUpdate32Val,

			&canonizedDebeziumDelete0Key,
			&canonizedDebeziumDelete0Val,

			&canonizedDebeziumDelete1Key,
		},
	)
	require.NoError(t, err)

	fmt.Printf("canonizedDebeziumInsertKey=%s\n", canonizedDebeziumInsertKey)
	fmt.Printf("canonizedDebeziumInsertVal=%s\n", canonizedDebeziumInsertVal)

	fmt.Printf("canonizedDebeziumUpdate1Key=%s\n", canonizedDebeziumUpdate1Key)
	fmt.Printf("canonizedDebeziumUpdate1Val=%s\n", canonizedDebeziumUpdate1Val)

	fmt.Printf("canonizedDebeziumUpdate2Key=%s\n", canonizedDebeziumUpdate2Key)
	fmt.Printf("canonizedDebeziumUpdate2Val=%s\n", canonizedDebeziumUpdate2Val)

	fmt.Printf("canonizedDebeziumUpdate30Key=%s\n", canonizedDebeziumUpdate30Key)
	fmt.Printf("canonizedDebeziumUpdate30Val=%s\n", canonizedDebeziumUpdate30Val)

	fmt.Printf("canonizedDebeziumUpdate31Key=%s\n", canonizedDebeziumUpdate31Key)

	fmt.Printf("canonizedDebeziumUpdate32Key=%s\n", canonizedDebeziumUpdate32Key)
	fmt.Printf("canonizedDebeziumUpdate32Val=%s\n", canonizedDebeziumUpdate32Val)

	fmt.Printf("canonizedDebeziumDelete0Key=%s\n", canonizedDebeziumDelete0Key)
	fmt.Printf("canonizedDebeziumDelete0Val=%s\n", canonizedDebeziumDelete0Val)

	fmt.Printf("canonizedDebeziumDelete1Key=%s\n", canonizedDebeziumDelete1Key)

	//------------------------------------------------------------------------------
	// start replication

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", Source, &target, abstract.TransferTypeSnapshotAndIncrement)

	mutex := sync.Mutex{}
	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		found := false
		for _, el := range input {
			if el.Table == "customers3" {
				found = true
			}
		}
		if !found {
			return
		}
		//---
		mutex.Lock()
		defer mutex.Unlock()

		for _, el := range input {
			if el.Table != "customers3" {
				continue
			}
			changeItems = append(changeItems, el)
		}
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements

	connParams, err := mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	db, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)

	_, err = db.Exec(insertStmt)
	require.NoError(t, err)
	_, err = db.Exec(update1Stmt)
	require.NoError(t, err)
	_, err = db.Exec(update2Stmt)
	require.NoError(t, err)
	_, err = db.Exec(update3Stmt)
	require.NoError(t, err)
	_, err = db.Exec(deleteStmt)
	require.NoError(t, err)

	for {
		time.Sleep(time.Second)

		mutex.Lock()
		if len(changeItems) == 9 {
			break
		}
		mutex.Unlock()
	}

	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[3].Kind, abstract.DoneShardedTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[5].Kind, abstract.UpdateKind)
	require.Equal(t, changeItems[6].Kind, abstract.UpdateKind)
	require.Equal(t, changeItems[7].Kind, abstract.UpdateKind)
	require.Equal(t, changeItems[8].Kind, abstract.DeleteKind)

	for i := range changeItems {
		fmt.Printf("changeItem dump: %s\n", changeItems[i].ToJSONString())
	}

	//-----------------------------------------------------------------------------------------------------------------
	// TM-4377

	parseTimestamp := func(t *testing.T, timeStr string) time.Time {
		timeVal, err := time.Parse("2006-01-02T15:04:05Z", timeStr)
		require.NoError(t, err)
		return timeVal
	}
	changeItems[5].ColumnValues[48] = parseTimestamp(t, "1999-01-01T00:00:01Z")

	changeItems[6].ColumnValues[48] = parseTimestamp(t, "1999-01-01T00:00:01Z")
	for i := range changeItems[6].OldKeys.KeyNames {
		if changeItems[6].OldKeys.KeyNames[i] == "timestamp_" {
			changeItems[6].OldKeys.KeyValues[i] = parseTimestamp(t, "1999-01-01T00:00:01Z")
			break
		}
	}

	changeItems[7].ColumnValues[48] = parseTimestamp(t, "1999-01-01T00:00:01Z")
	for i := range changeItems[7].OldKeys.KeyNames {
		if changeItems[7].OldKeys.KeyNames[i] == "timestamp_" {
			changeItems[7].OldKeys.KeyValues[i] = parseTimestamp(t, "1999-01-01T00:00:01Z")
			break
		}
	}

	changeItems[8].ColumnValues[48] = parseTimestamp(t, "1999-01-01T00:00:01Z")
	for i := range changeItems[8].OldKeys.KeyNames {
		if changeItems[8].OldKeys.KeyNames[i] == "timestamp_" {
			changeItems[8].OldKeys.KeyValues[i] = parseTimestamp(t, "1999-01-01T00:00:01Z")
			break
		}
	}

	//-----------------------------------------------------------------------------------------------------------------

	testSuite := []debeziumcommon.ChangeItemCanon{
		{
			ChangeItem: &changeItems[4],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumInsertKey,
				DebeziumVal: &canonizedDebeziumInsertVal,
			}},
		},
		{
			ChangeItem: &changeItems[5],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumUpdate1Key,
				DebeziumVal: &canonizedDebeziumUpdate1Val,
			}},
		},
		{
			ChangeItem: &changeItems[6],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumUpdate2Key,
				DebeziumVal: &canonizedDebeziumUpdate2Val,
			}},
		},
		{
			ChangeItem: &changeItems[7],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumUpdate30Key,
				DebeziumVal: &canonizedDebeziumUpdate30Val,
			}, {
				DebeziumKey: canonizedDebeziumUpdate31Key,
				DebeziumVal: canonizedDebeziumUpdate31Val,
			}, {
				DebeziumKey: canonizedDebeziumUpdate32Key,
				DebeziumVal: &canonizedDebeziumUpdate32Val,
			}},
		},
		{
			ChangeItem: &changeItems[8],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumDelete0Key,
				DebeziumVal: &canonizedDebeziumDelete0Val,
			}, {
				DebeziumKey: canonizedDebeziumDelete1Key,
				DebeziumVal: canonizedDebeziumDelete1Val,
			}},
		},
	}

	testSuite = testutil.FixTestSuite(t, testSuite, "dbserver1", "source", "mysql")

	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "dbserver1", "source", "mysql", false, testCase.DebeziumEvents)
	}

	for i := range testSuite {
		testSuite[i].ChangeItem = helpers.UnmarshalChangeItemStr(t, testSuite[i].ChangeItem.ToJSONString())
	}

	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "dbserver1", "source", "mysql", false, testCase.DebeziumEvents)
	}
}
