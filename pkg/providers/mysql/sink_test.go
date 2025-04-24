package mysql

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var random = rand.New(rand.NewSource(time.Now().Unix()))

const (
	sigma = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var jsons = []string{
	`'[1,2,3.012]'`,
	`
'{
	"object": {
		"another": {
			"wow such deep": {
				"really deep": [
					"vars",
					["more array", true, 1, 3.1415, "i love you"],
					true,
					false,
					"yes",
					"no"
				],
				"very deep": [1, 2, 3, 4.523312, 2.1]
			},
			"go fibonacci": [1, 1, 2, 3, 5, 8, 13, 21, 34, "enought pls"],
			"maybe factorial?": [1, 2, 3, 6, 24, 120, 720, 5040, "god are you growing so fast?", true, 1.011001, "ha, error"]
		},
	"another -almost-top-level obj": "it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. "
	},
	"god i am tired please let me go": "no, struggle"
}'
	`,
	`
'{
	"object": {
		"another": {
			"wow such deep": {
				"really deep": [
					"vars",
					["more array", true, 1, 3.1415, "i love you"],
					true,
					false,
					"yes",
					"no"
				],
				"very deep": [1, 2, 3, 4.523312, 2.1]
			},
			"go fibonacci": [1, 1, 2, 3, 5, 8, 13, 21, 34, "enought pls"],
			"maybe factorial?": [1, 2, 3, 6, 24, 120, 720, 5040, "god are you growing so fast?", true, 1.011001, "ha, error"]
		},
	"another -almost-top-level obj": "it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. it will be boring, but very long string. "
	},
	"god i am tired please let me go": "no, struggle"
}'
	`,
	`
'[
  "shade",
  [
    -205721087,
    [
      "tiny",
      false,
      -554863815,
      -1319267577.1675735,
      [
        false,
        {
          "political": true,
          "giving": "opposite",
          "church": "here",
          "let": [
            498558874,
            true,
            {
              "child": [
                [
                  true,
                  {
                    "grown": 955669643,
                    "got": [
                      false,
                      "blanket",
                      "fell",
                      [
                        1635838019.3084087,
                        [
                          -1179390876.7163591,
                          {
                            "different": {
                              "close": 897592122,
                              "many": {
                                "source": "rhythm",
                                "pleasant": -38638257.72750831,
                                "disease": 940214468,
                                "character": "feed",
                                "scale": {
                                  "wire": [
                                    [
                                      {
                                        "recognize": -1988325470,
                                        "progress": -1832930369,
                                        "important": true,
                                        "body": "arrive",
                                        "who": -1218284924,
                                        "afraid": "finally"
                                      },
                                      true,
                                      "frequently",
                                      "bark",
                                      "social",
                                      true
                                    ],
                                    "eat",
                                    "total",
                                    false,
                                    true,
                                    -283786607
                                  ],
                                  "vowel": -1006302023,
                                  "gasoline": false,
                                  "favorite": "dream",
                                  "noun": "flame",
                                  "everyone": "late"
                                },
                                "shape": false
                              },
                              "therefore": 307646545,
                              "drop": 1673894557,
                              "hurt": 521712853,
                              "series": true
                            },
                            "wall": -218891308,
                            "green": 1825839144,
                            "column": false,
                            "whether": 120920835.33114386,
                            "remove": "dig"
                          },
                          472784929.4639015,
                          "ocean",
                          -502171851,
                          "easily"
                        ],
                        true,
                        297105306.6692407,
                        1646862493.730905,
                        false
                      ],
                      577180561,
                      "barn"
                    ],
                    "shall": "lonely",
                    "was": false,
                    "result": "property",
                    "addition": -1586445791.7660887
                  },
                  -1110945138,
                  "slabs",
                  true,
                  false
                ],
                -1471714650,
                "down",
                "science",
                true,
                1862316518.4758065
              ],
              "this": true,
              "wolf": "firm",
              "poem": -2117059615.677954,
              "scientific": -1671425090,
              "travel": false
            },
            "further",
            "born",
            "element"
          ],
          "article": 1350688264.728907,
          "cover": 1600625384.3051262
        },
        -804252680,
        false,
        "crop",
        false
      ],
      false
    ],
    "bare",
    false,
    true,
    "fat"
  ],
  -1628461685.243575,
  false,
  true,
  -695934140.1606753
]
'`,
	`
'  {
    "_id": "5e55576d721e4f3a3f2ccb8b",
    "index": 0,
    "guid": "f1abf8fb-8cde-4d64-a624-68ff82d513de",
    "isActive": false,
    "balance": "$3,933.08",
    "picture": "http://placehold.it/32x32",
    "age": 35,
    "eyeColor": "blue",
    "name": {
      "first": "Allyson",
      "last": "Holden"
    },
    "company": "STEELFAB",
    "email": "allyson.holden@steelfab.io",
    "phone": "+1 (834) 437-2001",
    "address": "486 Duryea Court, Tuskahoma, Hawaii, 3940",
    "about": "Dolor nostrud aliqua dolor nulla ad officia laborum excepteur qui adipisicing esse in. Occaecat fugiat officia aliqua pariatur consectetur fugiat voluptate ullamco elit Lorem. Aliquip minim quis fugiat commodo aliquip aute officia deserunt officia occaecat duis incididunt sunt culpa.\n\nCulpa velit excepteur aute amet fugiat id consequat enim nostrud elit sit magna sit sint. Excepteur amet mollit velit fugiat commodo est qui aute cillum eiusmod aliqua aliqua. Esse nulla pariatur proident cillum magna reprehenderit in aute deserunt cillum enim id qui. Cillum aute ipsum duis deserunt laborum. Fugiat veniam consequat irure anim. Fugiat eu eu deserunt sint exercitation consequat nostrud incididunt.\n\nNostrud officia irure mollit officia est pariatur adipisicing ex. Dolor ea tempor elit amet officia cillum eiusmod labore ipsum non elit. Aliquip nostrud veniam magna nostrud Lorem est nulla quis. Id quis ut irure culpa.\n\nMinim exercitation duis sit adipisicing fugiat esse esse. Incididunt irure ex cupidatat anim quis magna reprehenderit sint nostrud Lorem. Consequat est pariatur dolore nulla nulla adipisicing incididunt incididunt anim. Nostrud cillum minim esse laboris quis reprehenderit incididunt id sit do reprehenderit occaecat do enim.\n\nNulla tempor eu incididunt sit. Sunt do laboris aute sunt irure enim eiusmod magna aliqua nisi sit proident. Incididunt ipsum sit eiusmod aliquip minim aliquip officia.\n\nSunt veniam cupidatat cillum nostrud occaecat tempor fugiat sint. Proident velit laborum labore tempor culpa amet nostrud laboris. Esse dolor culpa nisi exercitation consequat proident et. Nisi dolor do ipsum dolor dolore pariatur adipisicing reprehenderit in fugiat.\n\nDolore labore enim id ut aliquip ipsum ea veniam consequat id ut eiusmod elit. Aliquip nostrud id laboris ut culpa veniam nisi nostrud deserunt qui. Ex voluptate Lorem adipisicing cillum. Culpa minim Lorem magna ullamco do. Tempor laboris veniam occaecat incididunt commodo excepteur ea pariatur.\n\nPariatur enim cupidatat quis pariatur mollit qui et exercitation irure. Consectetur minim consectetur Lorem magna laborum cillum non cupidatat occaecat. Elit id exercitation eiusmod sunt est. Cillum nisi nisi sunt velit dolor labore enim consectetur ut voluptate ipsum excepteur magna. Deserunt dolore culpa elit ex velit nostrud nostrud elit culpa enim.\n\nPariatur sunt minim culpa sunt esse dolor enim irure officia veniam dolore tempor mollit. Ipsum occaecat amet sint dolore ut ex id eu duis in dolore velit. Ullamco aliqua pariatur eu adipisicing mollit mollit quis tempor nulla aute mollit id sunt sint. Ad voluptate irure labore ullamco eu officia enim nostrud consequat amet fugiat adipisicing commodo elit. Eiusmod sunt veniam ullamco amet cillum fugiat ex. Magna id dolor id laboris Lorem elit qui mollit. Ex et voluptate ipsum veniam pariatur incididunt sit sint est et.\n\nVelit velit pariatur ipsum officia amet officia Lorem reprehenderit adipisicing nostrud voluptate est. Et minim anim officia aliquip laborum ullamco laboris aliqua laboris id ex eu ea. Minim exercitation cupidatat esse nostrud aliquip reprehenderit exercitation aliquip ea id elit veniam. Do aliqua aliqua esse elit culpa enim irure pariatur anim.\n\nEt ad exercitation proident est duis sunt enim sint irure laborum excepteur eu irure ut. Culpa laborum anim eu sunt laborum velit voluptate esse sit id laboris. Id ea minim sunt reprehenderit esse do sunt eu. Consequat cupidatat voluptate ipsum incididunt esse labore. Lorem officia nisi ad duis.\n\nPariatur velit proident veniam laborum cupidatat sint consequat irure sint exercitation ut qui. Esse in Lorem esse exercitation pariatur sint excepteur ut. Deserunt nisi dolore irure eiusmod occaecat id ut deserunt magna ea id dolore. Eu adipisicing Lorem eiusmod sunt enim ipsum fugiat aliqua cupidatat qui.\n\nCupidatat officia occaecat exercitation in consequat duis aliqua ad enim ex Lorem. Cupidatat adipisicing do veniam eiusmod. Duis ut non do ut cillum cupidatat cillum nostrud pariatur voluptate Lorem nisi elit commodo. Consequat eiusmod et cupidatat do esse.\n\nLorem deserunt ut adipisicing voluptate fugiat excepteur reprehenderit ullamco eiusmod ea. Aliqua duis labore laboris consequat cupidatat laboris laborum cillum sint pariatur et reprehenderit occaecat. Aliqua proident dolore et pariatur. Ea sit est ea duis cupidatat fugiat duis ex consequat quis consectetur aute. Fugiat cupidatat consequat est est eiusmod duis do voluptate excepteur et sunt velit aliquip voluptate. Elit laborum veniam incididunt proident nulla anim. Adipisicing reprehenderit est ipsum mollit eiusmod culpa eiusmod pariatur dolore anim amet.\n\nCulpa laborum do laborum minim non nostrud non non ad. Labore id ad enim voluptate ad qui excepteur sit velit mollit non exercitation cillum. Est sunt do deserunt fugiat labore pariatur aliquip dolor excepteur exercitation reprehenderit cupidatat. Ea anim aliqua voluptate exercitation dolore do ea excepteur eu mollit commodo.\n\nEst ipsum dolore amet ullamco incididunt anim. Incididunt aliqua labore dolor duis sunt laborum proident fugiat consectetur sit esse sunt. Ea occaecat mollit enim aliqua aute magna exercitation. Non magna labore consectetur culpa tempor occaecat est. Duis velit irure do ullamco anim ullamco aute incididunt.\n\nIncididunt eiusmod excepteur veniam Lorem ipsum mollit incididunt duis pariatur est cupidatat occaecat dolore. Incididunt sunt dolore in eiusmod do dolore consequat id irure tempor. Ex qui quis fugiat elit consequat dolor nisi. In minim voluptate elit commodo et exercitation exercitation cillum non veniam ullamco. Id ipsum cupidatat magna ea qui veniam aliquip.\n\nIpsum laborum adipisicing est nostrud consectetur pariatur ad nostrud esse et labore aute. Incididunt amet fugiat veniam aliquip cupidatat consequat eiusmod. Proident proident et consequat deserunt. Est ex esse ex minim irure cupidatat velit velit nostrud ad. Ut do duis ullamco ex proident consectetur irure aute.\n\nTempor sunt cillum do reprehenderit labore nostrud proident dolor exercitation ad esse Lorem labore commodo. Cillum aliqua aliquip occaecat ea id. Do proident velit dolor ex aliquip irure proident. Culpa esse ullamco eiusmod ipsum aute minim nostrud do amet non aute nostrud. Nulla in fugiat ullamco incididunt. Laboris quis anim exercitation ullamco tempor sit.\n\nNulla proident fugiat est irure. Est eu commodo velit veniam minim ex. Sint labore officia amet eiusmod dolore aliqua. Dolor ullamco quis nulla tempor dolor velit ut incididunt aliquip nostrud proident Lorem. Tempor ad sint reprehenderit ex tempor sunt irure deserunt amet aute Lorem. Sit nostrud aute et est est labore ea cupidatat.\n\nConsectetur in sit aliquip elit nostrud fugiat. Ut quis duis id proident dolor laboris aliqua aliquip consequat nostrud excepteur. Tempor est velit occaecat fugiat commodo sit magna duis nostrud dolore cillum.\n\nEsse voluptate et dolor anim cupidatat elit. Duis quis eu proident veniam pariatur ad voluptate eiusmod est. Reprehenderit aliquip ullamco magna ipsum adipisicing incididunt aute duis sint. Nostrud esse laborum cillum nisi fugiat sunt aliqua qui. Deserunt in ea sint est consectetur aliqua tempor anim. Ad eiusmod reprehenderit tempor exercitation in.\n\nId sint ipsum nostrud elit ad non velit tempor duis aute non eu. Amet tempor voluptate proident aliquip Lorem qui laboris irure anim ad exercitation Lorem veniam. Amet sit ex proident anim. Sunt dolore commodo Lorem enim adipisicing ad deserunt enim labore sit tempor quis adipisicing nulla. Deserunt Lorem amet ea eu duis tempor dolore ea labore pariatur. Cupidatat anim magna labore laborum exercitation tempor ea duis qui. Nulla officia non Lorem nostrud eu nulla ut quis.\n\nCommodo veniam esse deserunt eiusmod sit officia irure aliquip sint nulla nulla Lorem magna dolor. Sint occaecat reprehenderit aute commodo. Minim quis veniam voluptate Lorem aliqua excepteur magna cillum elit est labore.\n\nOccaecat quis tempor officia quis sit culpa incididunt adipisicing nisi laborum aliqua esse. Id commodo fugiat ad consequat dolor eiusmod incididunt et. In minim laboris nisi ex ipsum deserunt aliqua veniam velit incididunt qui elit. Excepteur ut fugiat occaecat ut elit cupidatat cillum ea ullamco excepteur mollit dolore.\n\nEa eiusmod deserunt culpa occaecat pariatur quis labore in sunt aute aliqua est ut ullamco. Eiusmod tempor sit excepteur non nulla consequat quis nostrud nostrud sit pariatur esse id. Occaecat id exercitation commodo occaecat sint incididunt nostrud nostrud reprehenderit aliqua culpa voluptate et adipisicing. Do deserunt exercitation dolor dolor ipsum nostrud duis do anim ex ad.\n\nLaborum culpa aute ipsum quis magna est. Et reprehenderit exercitation id laboris voluptate pariatur elit ea sint. Sit sunt eiusmod aliqua officia anim exercitation ad eiusmod. Exercitation ut sunt laboris cupidatat nostrud nostrud nulla eu dolor minim.\n\nNostrud ad amet do esse est occaecat amet eiusmod esse labore minim proident ea cupidatat. Reprehenderit non ullamco labore duis culpa mollit. Ut dolor sunt excepteur reprehenderit quis. Ex reprehenderit aliquip voluptate velit nostrud ut id ex ad ad. Sit aute culpa excepteur incididunt cupidatat ipsum consequat esse dolore eiusmod reprehenderit aliqua labore. Laborum et ipsum exercitation amet deserunt eu ullamco dolore irure ex veniam et. Culpa aute culpa nostrud nostrud deserunt magna.\n\nAnim ut incididunt proident duis ipsum cillum esse eu in est. Pariatur minim tempor fugiat incididunt. Enim pariatur nostrud ut cillum laboris labore eiusmod officia quis.\n\nIncididunt eu aliqua dolor et culpa sunt duis in ea adipisicing laborum fugiat. In nostrud voluptate esse dolor quis eu fugiat exercitation reprehenderit esse consequat incididunt. Commodo officia ullamco in cillum nisi cupidatat proident et id incididunt sit. Anim cupidatat eiusmod consectetur sit consequat est esse consequat non aliqua culpa nulla ex. Fugiat Lorem aliqua aliqua occaecat enim ea magna laboris nostrud quis. Laborum aliquip amet ex ex nostrud in nulla labore mollit cillum eiusmod aliquip pariatur. Velit aliqua aliquip incididunt adipisicing aliqua officia aute velit Lorem.\n\nMollit mollit dolore tempor cupidatat Lorem aute commodo laboris eu ipsum tempor tempor sint ut. Laborum Lorem magna nulla incididunt velit pariatur magna consequat exercitation pariatur do. Velit anim magna laboris duis elit ullamco quis fugiat proident officia eu ad reprehenderit.\n\nEt voluptate incididunt laborum ex et officia deserunt et veniam tempor proident. Fugiat mollit aute ex in eu incididunt. Excepteur tempor culpa minim est ipsum enim. Ea irure ad minim exercitation est in ea commodo. Elit tempor et est ea elit quis deserunt ut dolor nulla laboris culpa mollit.\n\nSint aliquip enim cupidatat laboris esse id. Ullamco pariatur et sit adipisicing laborum ullamco proident non magna Lorem id consectetur laborum commodo. Magna in aliquip tempor exercitation aliqua magna ea duis voluptate aliquip. Occaecat consectetur id id id excepteur et velit id duis.\n\nEnim cupidatat ex eu incididunt minim sint nulla eu laborum culpa in aliqua commodo labore. Ex consectetur labore aliquip ea ullamco ut nostrud dolor quis esse eiusmod voluptate irure. Non Lorem tempor ea excepteur et dolore ex consectetur ipsum magna veniam esse laborum nisi. Et duis consectetur id sunt eiusmod officia aute sit est velit aliqua enim excepteur amet.\n\nQui nostrud velit duis nisi veniam Lorem ipsum laboris consectetur amet elit proident sint. Do culpa nisi labore pariatur irure do aliqua tempor minim enim. Nulla sunt ipsum labore sunt aute veniam aliquip incididunt sint aliqua ea ipsum. Aliqua et consequat aliqua cillum velit cupidatat ea. Ea aute deserunt veniam proident labore tempor consequat est nostrud culpa duis ipsum nulla. Cupidatat sint amet minim aute mollit excepteur sunt adipisicing duis officia reprehenderit minim nisi.\n\nUt exercitation ad consectetur ullamco est ullamco ut velit culpa sunt veniam. In mollit ex aliqua tempor fugiat minim sunt exercitation. Minim ut ex ut aliquip qui. Velit do ipsum incididunt quis velit veniam dolor nostrud id do voluptate aliquip. Cillum tempor tempor sunt officia irure consectetur ut labore.\n\nUllamco deserunt aute sint aliquip ullamco elit consectetur. Nisi nisi non esse ipsum. In deserunt et qui culpa ad labore aliquip nisi culpa exercitation enim sunt. Incididunt aute consectetur do est mollit cupidatat culpa consectetur cillum. Nostrud sint fugiat ex enim ullamco. Ea eiusmod sint et id pariatur cillum ullamco aliquip ea cillum veniam minim. Aliquip est incididunt labore quis nulla aute pariatur nisi dolor laborum aliquip dolor proident.\n\nNulla nisi labore ex excepteur pariatur velit sunt id magna. Labore sunt laborum minim cupidatat incididunt fugiat aliquip. Est mollit ut sit velit exercitation id aliqua veniam cupidatat laborum. Ullamco tempor aliqua aute sunt labore anim do elit non eu commodo dolore. Est Lorem ut ea ea id reprehenderit culpa proident do irure in Lorem aliquip quis. Nulla consequat voluptate cillum aliquip veniam velit id incididunt. Velit incididunt incididunt consectetur do.\n\nMagna ipsum excepteur dolor dolore excepteur fugiat ipsum. Ipsum adipisicing esse aliquip duis. Nostrud nostrud do Lorem sunt.\n\nIn culpa minim qui duis ut adipisicing ex culpa sit. Nisi elit sunt minim pariatur qui. Magna minim proident proident culpa sunt irure in enim mollit minim sunt id occaecat pariatur.\n\nDuis proident nostrud fugiat cupidatat. Sunt aute incididunt quis laborum fugiat laboris. Et dolor in deserunt velit irure nulla culpa magna amet.\n\nLaboris esse commodo deserunt magna sunt eiusmod nostrud. Nulla ipsum sunt tempor ea incididunt nulla cillum. Reprehenderit enim pariatur reprehenderit enim.\n\nAdipisicing proident enim reprehenderit est eiusmod. Ea velit ad do anim Lorem sit reprehenderit dolor labore nisi aute deserunt enim sit. Ullamco commodo et voluptate ut occaecat. Eu do laboris minim nulla anim eiusmod sit velit occaecat magna. Pariatur minim in enim do et consectetur. Exercitation non anim non proident.\n\nLaboris magna et aliqua Lorem ullamco aute laborum quis anim sint consectetur. Exercitation ut anim consectetur anim id incididunt. Eu ullamco mollit quis ipsum minim laboris anim anim reprehenderit nisi. In veniam anim magna amet fugiat do mollit tempor velit enim duis. Commodo mollit aliquip proident esse irure et proident. Incididunt sunt voluptate anim do.\n\nUllamco ex adipisicing enim ut elit esse minim amet. Aute excepteur id occaecat anim occaecat aliquip esse veniam aliquip consequat ex. Velit dolor aliquip esse pariatur dolore laborum ut adipisicing consequat ipsum veniam culpa. Tempor ex laborum commodo eiusmod commodo. Esse reprehenderit nisi anim proident ea cupidatat aliquip excepteur do enim esse culpa aliqua.\n\nMollit deserunt consequat minim sunt consequat. Ut aute ipsum aliqua tempor commodo nostrud. Sint non minim id nisi exercitation do ea. Do officia velit sit voluptate laborum quis nulla cupidatat et nulla consectetur sint. Do laboris quis qui velit reprehenderit sit sint esse pariatur incididunt nulla esse. Occaecat id pariatur elit consectetur sunt non veniam. Excepteur ex nostrud eu qui pariatur sint mollit eiusmod ad reprehenderit culpa fugiat ut enim.\n\nCulpa ut do est elit et ex ipsum tempor cillum sunt dolor. In laborum pariatur excepteur fugiat ut labore cupidatat esse excepteur eu ex. Voluptate laboris fugiat commodo laboris.\n\nProident labore cupidatat minim officia voluptate do do commodo dolor laborum. Exercitation qui enim elit dolor ad exercitation reprehenderit reprehenderit laboris. Magna ad labore fugiat ad quis nostrud laboris. Culpa in elit cillum tempor ad ex incididunt labore. Proident anim occaecat nostrud Lorem labore est non elit excepteur voluptate nostrud irure. Ea ullamco ea anim sit fugiat occaecat ipsum deserunt cillum ut.\n\nConsectetur ad dolore mollit Lorem ut non voluptate id. Voluptate nisi consequat minim aliquip. Ullamco anim anim cupidatat qui est culpa officia ipsum in deserunt tempor sit. Ad amet ex sit ut aliquip quis id reprehenderit eu culpa et elit nisi. Id aliqua ea dolor officia laborum esse consequat cillum aliquip officia incididunt. Aliquip ut anim minim laboris cillum adipisicing esse sunt ea ea sunt sunt culpa excepteur. Laborum cupidatat quis sunt anim voluptate consectetur.\n\nQui voluptate ut ipsum magna sunt veniam laborum ex quis. Ullamco enim mollit aliquip nostrud veniam irure. Cillum est sunt anim laboris amet enim sit. Consectetur irure sunt mollit et aute et sint. Sint do nostrud duis veniam labore qui velit nulla qui. Reprehenderit incididunt laborum tempor ea.\n\nAd et est officia dolor laborum voluptate. Consequat officia pariatur sunt eiusmod laboris sunt irure voluptate mollit. Excepteur duis consequat tempor adipisicing mollit occaecat ex velit sint commodo esse. Dolor esse minim eu dolor labore proident mollit anim exercitation nisi dolore laborum minim. Velit est pariatur minim mollit aute fugiat laboris est cillum fugiat tempor qui non. Ut laboris Lorem eu tempor sit ex do exercitation et aute occaecat mollit.\n\nConsectetur pariatur exercitation irure qui nostrud laborum. Eu sint quis aliquip reprehenderit. Enim mollit anim in do. Nulla reprehenderit laborum non fugiat irure ex excepteur minim et excepteur reprehenderit voluptate labore. Enim consectetur aliqua non magna culpa non quis elit esse elit. Sunt incididunt ullamco mollit elit. Mollit adipisicing esse deserunt laboris deserunt cupidatat officia dolor qui irure qui fugiat laboris.\n\nOccaecat sint nulla fugiat eiusmod consectetur. In consequat ex eiusmod duis proident ut nostrud exercitation esse minim tempor irure deserunt enim. Ipsum ad voluptate Lorem enim reprehenderit velit culpa. Nisi duis labore sit minim nisi veniam excepteur eiusmod quis veniam ea nostrud. Aliqua officia aliqua laborum dolore cupidatat deserunt. Qui ut culpa veniam laboris incididunt excepteur irure.\n\nMagna tempor id occaecat cillum irure laborum. Qui sit qui nulla dolore. Mollit deserunt excepteur nulla minim elit. Labore nostrud culpa ipsum cillum incididunt aliqua reprehenderit eiusmod incididunt laboris ea adipisicing. Aliqua non laboris eu ut. Exercitation minim consequat exercitation adipisicing.\n\nSint enim aliquip incididunt adipisicing voluptate in eu minim nostrud mollit mollit Lorem. Aliqua nostrud eiusmod quis deserunt do aute minim ut irure. Sint quis non ullamco sit non aliquip minim ex reprehenderit pariatur.\n\nAmet ad veniam dolore ex occaecat laboris exercitation labore nulla in id duis reprehenderit. Non enim ex consectetur sunt consectetur ullamco velit culpa dolore culpa non esse voluptate. Cupidatat laboris cillum aliqua eiusmod esse pariatur laboris amet. Esse culpa eiusmod dolore in non do. Adipisicing non labore nulla ut. Ipsum excepteur pariatur occaecat sunt pariatur qui esse ipsum duis sunt.\n\nCupidatat id cupidatat consectetur deserunt labore quis officia dolore ex ex enim aliquip commodo. Quis laboris anim quis consectetur proident exercitation nisi officia reprehenderit cupidatat non dolor velit elit. Occaecat labore veniam Lorem commodo culpa do sint. Tempor cupidatat sit magna adipisicing.\n\nEu et deserunt consequat cillum nostrud laborum sunt culpa. Et ex veniam sint eu labore duis proident adipisicing culpa voluptate non sunt culpa elit. Officia laboris duis id qui dolore nostrud adipisicing laborum sunt veniam commodo. Anim incididunt et tempor duis aliquip esse voluptate elit exercitation proident exercitation laboris. Nostrud reprehenderit qui est officia ipsum. Aliquip irure adipisicing ea fugiat.\n\nId occaecat aute mollit in nisi sunt irure dolore nulla. Velit irure do commodo eiusmod. Cupidatat deserunt do deserunt aliquip aliqua ex. Fugiat consectetur labore reprehenderit pariatur aliquip.\n\nDolor voluptate id ex laboris non laboris. Amet reprehenderit ad adipisicing dolor laborum nulla elit. Fugiat excepteur veniam ipsum nostrud adipisicing aliquip ipsum eu. Elit culpa consequat eu cillum ullamco. Labore dolor esse ex nulla velit cillum laboris sunt dolore. Ut dolor cupidatat veniam deserunt incididunt tempor. Labore excepteur ex magna culpa nisi ex duis ipsum laborum.\n\nAmet duis consequat laboris id adipisicing do id aliqua. Aliquip officia sint mollit sit commodo cillum ullamco voluptate velit excepteur in. In qui elit ut qui tempor ipsum. Aute est sit pariatur cillum est nisi laborum eu. Mollit voluptate culpa duis irure ut veniam eu est pariatur est non laboris. Minim deserunt quis labore ex ut. Sunt non ut reprehenderit laboris esse ex mollit aute et laborum.\n\nMagna eu proident veniam et adipisicing. Consequat culpa anim ullamco non consectetur dolor sit eiusmod. Cillum consequat commodo occaecat occaecat culpa consectetur ex veniam adipisicing tempor. In culpa aliquip consectetur aliquip duis eu anim in dolore ipsum ipsum excepteur. Enim voluptate fugiat eiusmod duis amet consectetur ullamco quis nostrud ad consequat incididunt.\n\nEnim officia non aute qui deserunt excepteur veniam veniam. Tempor ullamco labore culpa fugiat laborum Lorem sint est velit aliqua incididunt. Ex labore ut dolor ea sint laborum officia nulla Lorem ullamco aliquip.\n\nMollit cillum quis ad do. Nulla nostrud commodo aliqua ut commodo ad id. Non duis esse occaecat cupidatat enim magna adipisicing pariatur excepteur ullamco nostrud elit. Cillum proident mollit mollit duis voluptate pariatur fugiat voluptate ullamco excepteur. Laborum laboris labore elit non ex enim occaecat irure veniam. Dolor reprehenderit tempor cupidatat deserunt tempor.\n\nReprehenderit occaecat eiusmod sunt reprehenderit officia sit officia ullamco fugiat eu labore officia duis. Laborum ullamco sunt veniam anim cupidatat elit fugiat in laborum pariatur. Magna do enim dolor commodo id esse incididunt quis nulla consectetur anim nisi.\n\nNon officia pariatur nisi et cillum duis proident. Excepteur minim adipisicing nostrud exercitation deserunt nulla do occaecat commodo fugiat irure. Duis consequat pariatur enim aliqua sint consequat dolor ex cupidatat. Ea ipsum occaecat consequat elit occaecat ex sunt dolor laborum. Sint tempor sunt aute ullamco ullamco nulla incididunt. Culpa cillum ut commodo deserunt cupidatat nostrud velit eiusmod qui cupidatat nisi minim laborum ipsum.\n\nDolor nulla labore occaecat ex deserunt exercitation consequat fugiat irure ut id in culpa. Tempor incididunt sint sit adipisicing in consequat Lorem culpa duis proident adipisicing irure culpa consectetur. Ea in sint id proident nisi dolor pariatur.\n\nCommodo est sunt quis tempor occaecat consequat anim deserunt eiusmod do culpa ex. Nostrud nisi eu voluptate ipsum pariatur veniam qui duis eu aute est quis veniam mollit. Incididunt incididunt ad et pariatur quis.\n\nCulpa sint aliquip nulla qui eiusmod ex est anim nostrud sit proident reprehenderit in. Commodo amet dolore cillum eu dolore qui nulla ut minim. Anim consectetur ipsum ex pariatur ea voluptate sint nisi deserunt sunt aliquip non incididunt quis. Reprehenderit nostrud in occaecat fugiat veniam ex do fugiat ut. Dolore et et officia sunt cillum do ipsum dolor sint consectetur cillum consequat quis.\n\nEst aliqua sit incididunt cupidatat irure eu aliquip excepteur nostrud veniam. Anim commodo ad consequat commodo amet veniam dolor do. Sint culpa mollit velit cillum consectetur irure adipisicing irure dolore sit duis enim Lorem exercitation. Nisi exercitation sunt mollit enim reprehenderit Lorem elit. Nisi dolor exercitation culpa proident veniam laborum dolor in nisi veniam. Excepteur do tempor enim ex magna cillum nostrud eu laborum sint Lorem.\n\nEa veniam nostrud consectetur amet. Non adipisicing nostrud minim incididunt esse labore cupidatat Lorem aliqua aute minim ullamco. Qui consequat esse occaecat et nulla veniam eu voluptate. Eiusmod qui fugiat duis do consequat consequat ipsum sit aliqua commodo officia non labore. Officia sit sit anim occaecat est amet in commodo ex. Laborum ullamco aliqua non irure anim minim id non Lorem velit quis proident sint.\n\nFugiat cillum ipsum officia ea nulla pariatur cillum est proident. Consectetur dolore esse ipsum nostrud ut. Irure laboris officia adipisicing nisi commodo nostrud sunt non sint consectetur occaecat mollit. Ea adipisicing eiusmod qui consectetur ex. Dolore enim sunt sunt voluptate commodo. Culpa velit et exercitation dolore ex.\n\nLorem excepteur est elit proident velit. Exercitation est elit est dolore quis cupidatat ut incididunt proident occaecat ipsum elit. Dolor esse ipsum consequat dolor velit nisi adipisicing sunt. Id enim magna commodo in nulla ad id aliquip exercitation reprehenderit. Elit anim dolore exercitation eu sunt esse ullamco et aliquip sit laborum duis irure. Occaecat consectetur elit mollit laboris excepteur aliquip aliquip exercitation non. Minim mollit laboris amet cupidatat magna fugiat.\n\nNostrud irure aliquip quis qui mollit. Laboris officia ut amet ullamco. Ad qui cillum adipisicing consectetur consequat. Nostrud qui incididunt ipsum cillum officia veniam fugiat ex voluptate ea sunt commodo. Quis est proident aliqua ea pariatur sit magna ea. Ea et magna esse ut officia fugiat.\n\nUllamco ex id velit veniam ullamco id est sit ullamco aute sunt qui. Commodo mollit minim eiusmod mollit ullamco eu do magna cupidatat adipisicing veniam duis labore. Nisi aliquip proident tempor culpa elit culpa Lorem eu officia occaecat eiusmod sit elit. Nulla pariatur adipisicing mollit in culpa mollit proident Lorem sunt culpa nisi magna. Sunt culpa eiusmod esse duis laborum sint proident sint deserunt et labore. Laboris dolor veniam nisi dolor enim enim esse esse duis pariatur est. Ad do cillum nulla tempor sunt nulla.\n\nNon labore elit est elit ad cupidatat commodo irure pariatur dolore voluptate nisi deserunt amet. Enim tempor dolore commodo enim voluptate minim cillum do do consectetur excepteur excepteur. Excepteur aliquip ipsum aliqua ipsum consequat laborum magna sit. Dolor proident sunt incididunt eu proident mollit et id culpa consectetur. Velit cillum irure adipisicing irure elit irure ullamco nostrud dolor non consectetur sint quis aute. Incididunt sunt consequat laborum proident aute. Ea aliquip ut esse duis amet anim adipisicing aliqua deserunt mollit sunt.\n\nLorem officia consequat veniam in aliquip eiusmod est mollit labore ad. Cupidatat exercitation ad proident consequat adipisicing Lorem aliqua enim. Magna ad voluptate eu exercitation culpa anim.\n\nEx nisi veniam do deserunt. Tempor fugiat non est duis pariatur minim in deserunt ad. Ea exercitation culpa excepteur qui et quis tempor adipisicing dolore. Sint ex et veniam qui nostrud. Excepteur tempor consectetur qui non voluptate dolore nisi sint ullamco eiusmod elit ut sint proident. Elit ut id cupidatat adipisicing Lorem magna sit incididunt non ea.\n\nIn nostrud nisi reprehenderit ex exercitation nulla eu id ex labore. Mollit dolor ipsum pariatur eiusmod excepteur incididunt deserunt ullamco. Est veniam sit mollit ad proident in ut officia esse magna.\n\nCommodo minim nisi do enim nulla mollit nulla laborum culpa occaecat excepteur voluptate tempor voluptate. Exercitation enim do velit sint ea eu eiusmod proident dolor ipsum. Amet esse pariatur do ad. Magna duis irure reprehenderit pariatur ipsum exercitation quis. Occaecat Lorem nisi occaecat id Lorem sint incididunt ex amet incididunt. Et ex non exercitation occaecat qui magna exercitation ut commodo reprehenderit adipisicing laborum officia quis.\n\nDuis irure cillum dolor irure aliquip esse ipsum exercitation consectetur est. Cupidatat anim reprehenderit anim excepteur dolore labore duis eu. Aute ut aliqua minim amet sunt in labore ea.\n\nProident magna sint aliquip culpa officia et. Quis veniam anim commodo dolor do nulla sit proident. Reprehenderit duis nisi id ea aute enim. Aute sunt consequat tempor et adipisicing aliqua eu nisi sunt id non aliquip voluptate nostrud. Laborum est duis ipsum dolor fugiat aliquip ex deserunt. Sit aliquip aliquip et quis ut sit in.\n\nDuis deserunt nisi veniam commodo eu nulla cillum occaecat tempor laborum. Culpa qui ipsum ad dolore in. Id occaecat anim sint et cupidatat labore Lorem reprehenderit eiusmod.\n\nMollit velit duis culpa cillum laboris. Cillum mollit nostrud id duis laborum mollit velit sint dolore tempor tempor qui. Dolor ullamco consectetur culpa sunt pariatur irure. Non ullamco labore occaecat aliquip minim minim officia occaecat enim amet. Est labore aute sint nostrud sunt in sit id ullamco cupidatat ex pariatur magna id.\n\nSint magna commodo incididunt labore eu proident sit ex. In irure ullamco ullamco irure laborum. Eiusmod officia esse consequat aliquip laboris nostrud minim consequat do sit minim et voluptate sunt. Dolor labore non cupidatat labore minim consequat laborum ut minim proident.\n\nMinim nulla ea in deserunt ad id eiusmod elit minim incididunt. Velit culpa commodo velit minim sunt commodo fugiat exercitation aliqua. Fugiat adipisicing eiusmod minim consectetur eu commodo sit.\n\nQuis ex commodo aliqua irure quis culpa ullamco pariatur esse do minim est. Ut nisi eu sunt voluptate qui ullamco. Aliqua aute aliqua amet exercitation ea voluptate est mollit ullamco ea ipsum velit id nisi. Cupidatat Lorem et ipsum culpa cupidatat qui in et. Consequat sunt aliquip proident deserunt proident sunt mollit id nisi laboris nulla enim officia aliqua. Eu amet pariatur velit quis consectetur laborum magna.\n\nAute eu ipsum culpa veniam minim ut ut nisi. Minim in proident enim labore aliquip do duis dolore velit et duis tempor exercitation deserunt. Nostrud cupidatat cillum culpa esse sunt aute deserunt aliquip aliquip duis deserunt et esse.\n\nAd consectetur laboris tempor ullamco occaecat commodo reprehenderit pariatur in dolor qui pariatur Lorem aute. Eu culpa Lorem aute consequat reprehenderit. Qui eu magna ullamco aute pariatur elit mollit in nulla sunt tempor nisi quis. Pariatur voluptate culpa reprehenderit proident consectetur quis dolor aliquip.\n\nQuis laboris sit ut sint. Non quis ullamco aliquip proident ad enim nisi ut ex fugiat laborum tempor. Ea esse elit minim velit esse veniam irure exercitation aliqua elit. Aliquip consectetur id reprehenderit adipisicing magna.\n\nDolore magna duis minim aute amet sit sunt laboris. Qui elit amet proident sint sint qui ex ut. Id ullamco irure elit dolore esse commodo cillum. Officia elit tempor voluptate id quis excepteur voluptate quis ea consequat.\n\nDolore reprehenderit ut veniam mollit non ad occaecat irure consectetur proident Lorem aute ut. Amet dolore pariatur cupidatat mollit officia consectetur laborum consequat quis commodo aliqua anim. Consectetur aute cupidatat quis et nulla sunt laboris laboris mollit est.\n\nEx Lorem elit in est non commodo anim ea quis sunt. Eu in fugiat officia enim dolore. Labore ea ea ut reprehenderit ipsum ex aute et nostrud ut eu eu deserunt. Culpa nulla et quis non minim ullamco labore.\n\nIn enim ea dolore id et Lorem aliquip proident enim ad labore excepteur voluptate ullamco. Pariatur eu dolore commodo laborum culpa id nostrud. Adipisicing occaecat pariatur sunt anim fugiat laborum sint exercitation qui cupidatat quis ipsum incididunt dolore. Id eiusmod in elit qui proident consectetur deserunt exercitation veniam dolor dolor. Est pariatur sunt irure labore ea irure cillum reprehenderit mollit quis voluptate. Adipisicing duis excepteur exercitation veniam ea nulla reprehenderit commodo nulla.\n\nEx exercitation cillum deserunt eu ipsum aliquip tempor. Non nulla enim tempor labore dolor deserunt quis. Cillum ea laboris Lorem ut cupidatat velit Lorem enim fugiat elit ipsum duis. Irure eu incididunt ut amet id culpa consectetur cupidatat id. Id labore aliqua excepteur nostrud.\n\nDuis labore consequat laborum consequat. Tempor eu consequat irure id minim. Elit Lorem non magna labore. Eiusmod mollit magna id sit. Et nostrud in veniam mollit aliquip velit consectetur do sint deserunt occaecat. In pariatur laboris aute tempor Lorem voluptate incididunt commodo ut officia. Quis nulla irure id aute eiusmod amet ullamco est qui fugiat laborum.\n\nQuis laboris ex qui ad elit aute eu sunt. Dolor adipisicing ex fugiat ex aliquip consectetur consequat eiusmod aute dolor enim proident duis officia. Adipisicing quis fugiat ad ad pariatur elit excepteur ullamco nostrud et enim nulla.\n\nIrure sint voluptate commodo magna nostrud et Lorem occaecat reprehenderit voluptate consectetur. Velit eu mollit esse do pariatur. Ea irure id velit non incididunt magna velit Lorem aliquip. Enim cillum cupidatat consectetur do et occaecat quis exercitation nostrud. Dolor ad eiusmod voluptate adipisicing commodo.\n\nCommodo non consequat excepteur mollit veniam cillum enim elit veniam eiusmod. Velit consectetur magna deserunt aliquip occaecat minim non ea consequat mollit aliquip amet excepteur. Magna enim veniam excepteur nulla sunt nulla ullamco non esse voluptate consequat reprehenderit. Ea dolore fugiat labore eu. Reprehenderit nulla commodo dolore culpa pariatur eiusmod commodo ex. Fugiat exercitation est est adipisicing anim. Reprehenderit amet velit deserunt excepteur cupidatat proident irure ipsum reprehenderit cupidatat.\n\nElit minim elit pariatur velit. Sint laboris occaecat officia magna anim sunt ad. Occaecat enim duis duis anim incididunt. Id labore commodo aliqua officia adipisicing ad. Pariatur est fugiat ut esse cillum eiusmod officia occaecat. Id ipsum laborum ut magna culpa nisi tempor qui.",
    "registered": "Sunday, November 2, 2014 11:33 PM",
    "latitude": "76.039475",
    "longitude": "-150.018717",
    "tags": [
      "sint",
      "ad",
      "non",
      "veniam",
      "qui"
    ],
    "range": [
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ],
    "friends": [
      {
        "id": 0,
        "name": "Chan Case"
      },
      {
        "id": 1,
        "name": "Cook Rojas"
      },
      {
        "id": 2,
        "name": "Francine Armstrong"
      }
    ],
    "greeting": "Hello, Allyson! You have 9 unread messages.",
    "favoriteFruit": "apple"
  }'
`,
	// TODO append more testing jsons
}

func randStr(sigma string, maxLen int) string {
	length := random.Int()%maxLen + 1
	res := make([]byte, length)
	for i := range res {
		res[i] = sigma[random.Intn(len(sigma))]
	}
	return string(res)
}

func generate(pgType string) (interface{}, error) {
	now := time.Now()
	pgType = strings.TrimPrefix(pgType, "mysql:")
	switch pgType {
	case "date":
		return now, nil
	case "varchar":
		return randStr(sigma, 1), nil
	case "bigint":
		return random.Int63n(9007199254740991), nil
	case "integer":
		return random.Int31(), nil
	case "smallint":
		return int16(random.Int()), nil
	case "real", "numeric", "double precision":
		return random.Float64(), nil
	case "boolean":
		return random.Intn(2) != 0, nil
	case "json":
		return jsons[random.Intn(len(jsons))], nil
	case "jsonb":
		return jsons[random.Intn(len(jsons))], nil
	}
	return "", xerrors.New("no matching for such type: " + pgType)
}

func generateRow(schema []abstract.ColSchema) ([]string, []interface{}, error) {
	names := make([]string, len(schema))
	values := make([]interface{}, len(schema))
	for cnt, v := range schema {
		names[cnt] = v.ColumnName
		var err error
		values[cnt], err = generate(v.OriginalType)
		if err != nil {
			return nil, nil, err
		}
	}
	return names, values, nil
}

func BenchmarkSinker_represent(b *testing.B) {
	schema := []abstract.ColSchema{
		{ColumnName: "id", OriginalType: "bigint", PrimaryKey: true},
		{ColumnName: "json", OriginalType: "json"},
		{ColumnName: "boolean", OriginalType: "boolean"},
		{ColumnName: "varchar", OriginalType: "varchar"},
		{ColumnName: "integer", OriginalType: "integer"},
		{ColumnName: "smallint", OriginalType: "smallint"},
		{ColumnName: "real", OriginalType: "real"},
		{ColumnName: "numeric", OriginalType: "numeric"},
		{ColumnName: "bigint", OriginalType: "bigint"},
		{ColumnName: "date", OriginalType: "date"},
	}
	names, vals, _ := generateRow(schema)
	for i, n := range names {
		b.Run(n, func(b *testing.B) {
			r := CastToMySQL(vals[i], schema[i])
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				r := CastToMySQL(vals[i], schema[i])
				require.True(b, len(r) >= 0)
			}
			b.SetBytes(int64(len(r)))
			b.ReportAllocs()
		})
	}
	b.Run("null", func(b *testing.B) {
		r := CastToMySQL(nil, schema[0])
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			r := CastToMySQL(nil, schema[0])
			require.True(b, len(r) >= 0)
		}
		b.SetBytes(int64(len(r)))
		b.ReportAllocs()
	})
}

func BenchmarkSinker_buildQuery(b *testing.B) {
	s := sinker{
		db:      nil,
		metrics: stats.NewSinkerStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		config: &MysqlDestination{
			Password:  "",
			Host:      "",
			User:      "",
			ClusterID: "",
		},
		logger: logger.Log,
	}
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", OriginalType: "bigint", PrimaryKey: true},
		{ColumnName: "json", OriginalType: "json"},
		{ColumnName: "boolean", OriginalType: "boolean"},
		{ColumnName: "varchar", OriginalType: "varchar"},
		{ColumnName: "integer", OriginalType: "integer"},
		{ColumnName: "smallint", OriginalType: "smallint"},
		{ColumnName: "real", OriginalType: "real"},
		{ColumnName: "numeric", OriginalType: "numeric"},
		{ColumnName: "bigint", OriginalType: "bigint"},
		{ColumnName: "date", OriginalType: "date"},
	})
	rows := []abstract.ChangeItem{}
	for i := 0; i < 100; i++ {
		names, vals, _ := generateRow(schema.Columns())
		rows = append(rows, abstract.ChangeItem{
			Kind:         "insert",
			Schema:       "db",
			Table:        "test",
			ColumnNames:  names,
			ColumnValues: vals,
			TableSchema:  schema,
		})
	}
	tableID := abstract.TableID{
		Namespace: "db",
		Name:      "test",
	}
	st := time.Now()
	q, _ := s.buildQueries(tableID, schema.Columns(), rows)
	logger.Log.Infof("q size: %v in %v", format.SizeInt(len(q[0].query)), time.Since(st))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		q, _ := s.buildQueries(tableID, schema.Columns(), rows)
		require.True(b, len(q) > 0)
	}
	b.SetBytes(int64(len(q[0].query)))
	b.ReportAllocs()
}

func Test_generatedBounds(t *testing.T) {
	input := []string{
		"111111111111111111111111111111111111111111111111111111111111111111",
		"22222222222222222222222222222222222",
		"33333333333333333333333333333333333",
		"4444444444444444444444444444444444444444444444444444444444444444444",
		"555555555555555555555555555555",
	}
	bounds := generatedBounds(input, len(input[0])-1)
	logger.Log.Info("generated bounds", log.Any("bounds", bounds))
	for _, b := range bounds {
		logger.Log.Infof("combined query: %v->%v: %v", b.Left, b.Right, input[b.Left:b.Right])
	}
	require.Equal(t, []abstract.TxBound{
		{Left: 0, Right: 1},
		{Left: 1, Right: 3},
		{Left: 3, Right: 4},
		{Left: 4, Right: 5},
	}, bounds)
}

// checking that if the table cannot be created, a fatal error is returned
func Test_create_table(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// error on any Exec
	mock.ExpectExec(regexp.QuoteMeta(".")).
		WillReturnError(xerrors.New("an exec error occurred"))
	sink := &sinker{
		cache:  map[abstract.TableID]*abstract.TableSchema{},
		db:     db,
		logger: logger.Log,
	}
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "columnName", OriginalType: "utf8"}})
	err = sink.createTable(
		changeitem.TableID{
			Namespace: "namespace",
			Name:      "name",
		}, tableSchema)
	require.True(t, abstract.IsFatal(err))
}
