<?php

namespace App\Http\Controllers\Dsp;

use App\Http\Controllers\Controller;
use App\Http\Controllers\SSH2;
use App\Models\ClickHouse\DspInfoStats;
use App\Models\Adm;
use App\Models\Adult;
use App\Models\AllowedSize;
use App\Models\BalanceHistory;
use App\Models\BaseModel;
use App\Models\BlockedSites;
use App\Models\BrokenCrids;
use App\Models\CookieSyncPartners;
use App\Models\CookieSyncPartnersMap;
use App\Models\DspHourlyStats;
use App\Models\DspSspComments;
use App\Models\DspVastMacroses;
use App\Models\ExternalApiLinksDSP;
use App\Models\ListDSP;
use App\Models\ListSSP;
use App\Models\NurlErrors;
use App\Models\PrebidPartners;
use App\Models\Project;
use App\Models\Rates;
use App\Models\RequestExamples;
use App\Models\ResponseExamples;
use App\Models\RubiconCustomDspInputs;
use App\Models\Servers;
use App\Models\SettingsDSP;
use App\Models\SettingsDspAdditionalFields;
use App\Models\SettingsDspPrebid;
use App\Models\SettingsSSP;
use App\Models\SspTrafficQuality;
use App\Models\StatImpression;
use App\Models\Stats;
use App\Models\TrafficQualityTypes;
use App\Models\User;
use App\Services\DSP\AllowedRegionsService;
use App\Services\PrebidSettings\PrebidConfigService;
use Carbon\Carbon;
use ClickHouseDB\Client;
use DateInterval;
use DateTime;
use DateTimeZone;
use Exception;
use Illuminate\Http\Request;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Cookie;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use JsonSchema\Constraints\Constraint;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use function array_filter;
use function array_merge;
use function array_unique;
use function in_array;
use function is_array;
use function is_string;
use function json_decode;
use const JSON_FORCE_OBJECT;
use function app;
use function dd;

class DspController extends Controller
{

    const COUNTPAGINTRADEOPTIONS = 100;
    const MACROSNOTCHANGED = 'Macros were not changed';

    private $actionAjax = [
        'getExamples',
        'addBlockedCrid',
        'deleteBlockedCrid',
        'setCompanyComment',
        'getCompanyComment',
    ];

    private $preferentsByRole = [
        "adaptraffic" => "_isCustomManager",
    ];

    private bool $changeAllMacroses = true;
    private array $notChangeMacroses = [];

    public function __construct()
    {
        //$this->middleware('auth');
    }

    public function action(Request $request)
    {
        if (empty($request->action) || !in_array($request->action, $this->actionAjax)) {
            return false;
        }
        $action = $request->action;
        return $this->$action($request);
    }


    public function index(SettingsDSP $settingsDSP, Rates $rates, Servers $servers)
    {
        $cookieStatus = Cookie::get('tbl_dsp_status');
        $cookieIntegration = Cookie::get('tbl_dsp_integration');

        $statusInt = getTblStatusDsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $integration = getTblIntegration($cookieIntegration);
        $statusIntegration = getTxtIntegration($cookieIntegration);

        $now = new \DateTime();
        $week_ago = new \DateTime();
        $week_ago->modify('-1 day');
        $yesterday = $week_ago->format('Y-m-d');
        $week_ago->modify('-5 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');

        $listDSP = $settingsDSP->getListDspWithManager($statusInt, $integration);
        $listManagers = getListCompanyManager($listDSP);

        $winRate = $rates->getWinRate('dsp');
        $listRegions = $servers->getAllRegions();

        return view('dsp.list', [
            'statusTxt' => $statusTxt,
            'statusIntegration' => $statusIntegration,
            'dateFrom' => $from,
            'dateTo' => $to,
            'listDsp' => $listDSP,
            'listManagers' => $listManagers,
            'activeManager' => Cookie::get('activeManagerDsp'),
            'winRate' => $winRate,
            'listRegions' => $listRegions,
            'selectedRegion' => Cookie::get('selectedRegionDsp') ?? false,
        ]);
    }

    public  function indexVue()
    {
        $title = 'DSP';
        return view('vue', compact('title'));
    }

    public function getDataTable(Request $request, SettingsDSP $settingsDSP, Rates $rates, Servers $servers)
    {
        $cookieStatus = Cookie::get('tbl_dsp_status');
        $cookieIntegration = Cookie::get('tbl_dsp_integration');

        $statusInt = getTblStatusDsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $integration = getTblIntegration($cookieIntegration);
        $statusIntegration = getTxtIntegration($cookieIntegration);

        $cookieActiveManager = Cookie::get('activeManagerDsp');
        $cookieSelectedRegion = Cookie::get('selectedRegionDsp');
        $sortedBy = $request->get('sortedBy');
        $orderBy = $request->get('orderBy');
        $offset = $request->get('offset');
        $currentPage = (int)$request->page;
        $export = $request->has('export') ? $request->export : false;

        $now = new \DateTime();
        $week_ago = new \DateTime();
        $week_ago->modify('-1 day');
        $yesterday = $week_ago->format('Y-m-d');
        $week_ago->modify('-5 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');
        $filters = json_decode($request->get('filters'), true);
        $listDSP = $settingsDSP->getListDspWithManagerVue(
            $statusInt,
            $integration,
            $cookieSelectedRegion,
            $cookieActiveManager,
            $orderBy,
            $sortedBy,
            $filters,
            $offset
        );

        /*if (Cache::has('dsp.list.managers')) {
            $listManagers = Cache::get('dsp.list.managers');
        } else {
            $listManagers = (new User)->getListAccountManagers('dsp');
            Cache::put('dsp.list.managers', $listManagers, 24 * 60 * 60);
        }*/
        $listManagers = (new User)->getListAccountManagers('dsp');

        $winRate = $rates->getWinRate('dsp');
        $listRegions = $servers->getAllRegions();

        if ($export) {

            $listDSPFull = $listDSP['data'];

            while ($currentPage < $listDSP['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $listDSP = $settingsDSP->getListDspWithManagerVue(
                    $statusInt,
                    $integration,
                    $cookieSelectedRegion,
                    $cookieActiveManager,
                    $orderBy,
                    $sortedBy,
                    $filters
                );
                $listDSPFull = array_merge($listDSPFull, $listDSP['data']);
            }

            $listDSPFullWithWinRate = [];

            foreach ($listDSPFull as $listItem) {
                $currentKeyName = $listItem['keyname'];
                if (isset($winRate["$currentKeyName"])) {
                    $listItem['win_rate'] = $winRate["$currentKeyName"]['winRate'];
                } else {
                    $listItem += ['win_rate' =>  '0' ];
                }
                array_push($listDSPFullWithWinRate, $listItem);
            }

            $headers = [
                'ID',
                'Endpoint name',
                'Endpoint',
                'B',
                'N',
                'V',
                'A',
                'P',
                'Region',
                'Limit QPS',
                'Real QPS',
                'Spend Yesterday',
                'Spend Today',
                'Win Rate',
                'Company name'
            ];

            $ext = $request->has('ext') ? $request->ext : 'xlsx';
            $columns = [
                'id',
                'keyname',
                'endpoint',
                'usebanner',
                'usenative',
                'usevideo',
                'useaudio',
                'usepop',
                'region',
                'qps',
                'real_qps',
                'yesterdayspend',
                'dailyspend',
                'win_rate',
                'company_name',
            ];
            $this->exportToFile($headers, $columns, $listDSPFullWithWinRate, $ext, 2);

            return response('success', 200);

        } else {

            return response()->json([
                'statusTxt' => $statusTxt,
                'statusIntegration' => $statusIntegration,
                'dateFrom' => $from,
                'dateTo' => $to,
                'activeManager' => $listManagers[$cookieActiveManager]['name'] ?? null,
                'selectedRegion' => $cookieSelectedRegion,
                'listDsp' => $listDSP,
                'listManagers' => $listManagers ? array_values($listManagers) : null,
                'winRate' => $winRate,
                'listRegions' => $listRegions,
                'userRole'=> Auth::user()->role,
            ]);
        }
    }

    public function index2(SettingsDSP $settingsDSP, Rates $rates, Servers $servers)
    {
        $cookieStatus = Cookie::get('tbl_dsp_status');

        $statusInt = getTblStatusDsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);

        $now = new \DateTime();
        $week_ago = new \DateTime();
        $week_ago->modify('-1 day');
        $yesterday = $week_ago->format('Y-m-d');
        $week_ago->modify('-5 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');

        $listDSP = $settingsDSP->getListDspWithManager($statusInt);
        $listManagers = getListCompanyManager($listDSP);

        $winRate = $rates->getWinRate('dsp');
        $listRegions = $servers->getAllRegions();

        $listDspCompanyWithEP = $this->prepareListDspCompanyWithEndpoints($listDSP, $winRate);

        return view('dsp.list2', [
            'statusTxt' => $statusTxt,
            'dateFrom' => $from,
            'dateTo' => $to,
            'listDsp' => $listDspCompanyWithEP,
            'listManagers' => $listManagers,
            'activeManager' => Cookie::get('activeManagerDsp'),
            'winRate' => $winRate,
            'listRegions' => $listRegions,
            'selectedRegion' => Cookie::get('selectedRegionDsp') ?? false,
        ]);
    }

    public function getDataByCompany(Request $request, SettingsDSP $settingsDSP, DspList $dspList, Rates $rates, Servers $servers)
    {
        $cookieStatus = Cookie::get('tbl_dsp_status');

        $statusInt = getTblStatusDsp($cookieStatus);
        $statusTxt = getTxtStatus($cookieStatus);
        $sortedBy = $request->get('sortedBy');
        $orderBy = $request->get('orderBy');
        $offset = $request->get('offset');
        $currentPage = (int)$request->page;
        $cookieActiveManager = Cookie::get('activeManagerDsp');
        $cookieSelectedRegion = Cookie::get('selectedRegionDsp');

        $now = new \DateTime();
        $week_ago = new \DateTime();
        $week_ago->modify('-1 day');
        $yesterday = $week_ago->format('Y-m-d');
        $week_ago->modify('-5 day');
        $from = $week_ago->format('Y-m-d');
        $to = $now->format('Y-m-d');
        $filters = json_decode($request->get('filters'), true);
        $export = $request->has('export') ? $request->export : false;

        $listDSP = $settingsDSP->getListDspWithManager($statusInt);
        $listManagers = getListCompanyManager($listDSP);

        $winRate = $rates->getWinRate('dsp');
        $listRegions = $servers->getAllRegions();

        $listDspCompanyWithEP = $dspList->getEndpointsByDspCompany($request, $statusInt, $statusTxt, $cookieSelectedRegion, $cookieActiveManager, $orderBy, $sortedBy, $filters, $winRate);

        if ($export) {

            $listDSPCompanyFull = $listDspCompanyWithEP['data'];

            while ($currentPage < $listDspCompanyWithEP['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $listDSPCompany = $dspList->getEndpointsByDspCompany(
                    $request,
                    $statusInt,
                    $statusTxt,
                    $cookieSelectedRegion,
                    $cookieActiveManager,
                    $orderBy,
                    $sortedBy,
                    $filters,
                    $winRate
                );
                $listDSPCompanyFull = array_merge($listDSPCompanyFull, $listDSPCompany['data']);
            }

            $listDSPFullWithWinRate = [];

            foreach ($listDSPCompanyFull as $companyItem) {
                $listDSPFullWithWinRate[] = $companyItem;
            }

            $headers = [
                'Name',
                'B',
                'N',
                'V',
                'A',
                'P',
                'Region',
                'Limit QPS',
                'Real QPS',
                'Bid QPS',
                'Spend Yesterday',
                'Spend Today',
                'Win Rate',
                'Spend Limit'
            ];

            $ext = $request->has('ext') ? $request->ext : 'xlsx';

            $columns = [
                'company_name',
                'usebanner',
                'usenative',
                'usevideo',
                'useaudio',
                'usepop',
                'region',
                'qps',
                'real_qps',
                'bid_qps',
                'yesterdayspend',
                'dailyspend',
                'winrate',
                'spendlimit'
            ];
            $this->exportToFile($headers, $columns, $listDSPFullWithWinRate, $ext, 2);

            return response('success', 200);

        } else {
            return response()->json([
                'statusTxt' => $statusTxt,
                'dateFrom' => $from,
                'dateTo' => $to,
                'listDsp' => $listDspCompanyWithEP,
                'listManagers' => $listManagers ? array_values($listManagers) : null,
                'activeManager' => $listManagers[$cookieActiveManager]['name'] ?? null,
                'winRate' => $winRate,
                'listRegions' => $listRegions,
                'selectedRegion' => Cookie::get('selectedRegionDsp') ?? null,
                'userRole'=> Auth::user()->role
            ]);
        }
    }

    private function prepareListDspCompanyWithEndpoints($listDSP, $winRate)
    {
        $listDspByCompany = [];
        foreach ($listDSP as $dspInfo) {
            $listDspByCompany[$dspInfo['company_name']]['company_name'] = $dspInfo['company_name'];
            $listDspByCompany[$dspInfo['company_name']]['endpoints'][] = $dspInfo;
        }

        foreach ($listDspByCompany as $company => &$row) {
            $row['usebanner'] = max(array_column($row['endpoints'], 'usebanner'));
            $row['usenative'] = max(array_column($row['endpoints'], 'usenative'));
            $row['usevideo'] = max(array_column($row['endpoints'], 'usevideo'));
            $row['adaptraffic'] = max(array_column($row['endpoints'], 'adaptraffic'));
            $row['allowVastRtb'] = max(array_column($row['endpoints'], 'allowVastRtb'));
            $regions = array_unique(array_column($row['endpoints'], 'region'));
            sort($regions);
            $row['regions'] = implode(',', $regions);
            $row['qps'] = array_sum(array_column($row['endpoints'], 'qps'));
            $row['real_qps'] = array_sum(array_column($row['endpoints'], 'real_qps'));
            $row['bid_qps'] = array_sum(array_column($row['endpoints'], 'bid_qps'));
            $row['yesterdayspend'] = array_sum(array_column($row['endpoints'], 'yesterdayspend'));
            $row['dailyspend'] = array_sum(array_column($row['endpoints'], 'dailyspend'));
            $WinRate = [];
            foreach ($row['endpoints'] as $ep) {
                $WinRate[] = $winRate[$ep['keyname']]['winRate'] ?? 0;
            }
            if (in_array(app()->project->name, [Project::ACUITY])) {
                $row['companyActive'] = max(array_column($row['endpoints'], 'active'));
                $sumActiveEndpoints = 0;
                $counter = 0;
                foreach ($row['endpoints'] as $k => $endpoint) {
                    if ($endpoint['active'] && $endpoint['qps']) {
                        $sumActiveEndpoints += $endpoint['actmarga'];
                        $counter++;
                    }
                }
                $row['avgActMarga'] = $counter ? round($sumActiveEndpoints/$counter) : 0;
            } else {
                $row['avgActMarga'] = round(array_sum(array_column($row['endpoints'], 'actmarga'))/count($WinRate));
            }


            $row['avgWinRate'] = array_sum($WinRate)/count($WinRate);
            $row['maxSpendLimit'] = max(array_column($row['endpoints'], 'spendlimit'));
        }

        return $listDspByCompany;
    }

    /**
     * @return \Illuminate\Contracts\View\Factory|\Illuminate\View\View
     */
    public function createVue()
    {
        $title = 'Create DSP Endpoint';

        return view('vue', compact('title'));
    }

    public function create(Request $request)
    {
        $errors = [];

        $dsp = new SettingsDSP();

        $integration = $request->has('integration') ? $request->get('integration') : 'rtb';
        switch ($integration) {
            case 'prebid': $dsp->usePrebid = 1; break;
            case 'xml'   : $dsp->usexml    = 1; break;
            case 'vast'  : $dsp->useVast    = 1; break;
        }

        $dsp->company_name = $request->has('companyName') ? $this->prepareCompanyName($request->get('companyName')) : '';
        $dsp->endpoint = $request->has('endpointUrl') ? clean($request->get('endpointUrl')) : '';
        if ($integration === 'prebid') {
            switch (app()->project->name) {
                case Project::SMARTY_ADS:
                case Project::BIZZ_CLICK:
                    $dsp->endpoint = '88.214.194.152';
                    break;
                case Project::ACUITY:
                    $dsp->endpoint = '88.214.194.232:81/openrtb2/auction';
                    break;
                case Project::ACEEX:
                    $dsp->endpoint = '127.0.0.1';
                    break;
            }
        }
        $formats = $request->has('formats') ? array_filter(explode(',', $request->get('formats'))) : [];
        $dsp->mobweb = $request->has('mob') && $request->get('mob') ? 1 : 0;
        $dsp->desktop = $request->has('desktop') && $request->get('desktop') ? 1 : 0;
        $dsp->inapp = $request->has('inApp') && $request->get('inApp') ? 1 : 0;
        $dsp->ctvonly = $request->has('ctv') && $request->get('ctv') ? 1 : 0;
        $dsp->qps = $request->has('qps') ? intval($request->get('qps')) : 0;
        $dsp->spendlimit = $request->has('spendLimit') ? intval($request->get('spendLimit')) : 0;
        $dsp->maxbidfloor = $request->has('maxBidFloor') ? floatval($request->get('maxBidFloor')) : 0;
        $dsp->region = $request->has('region') ? $request->get('region') : '';
        $dsp->keyname = $request->has('endpointName') ? $this->prepareEpName($request->get('endpointName'), $dsp->region) : '';
        $dsp->allowedSSP = $dsp->blocked_ssp = $dsp->allowedCountries = '{}';

        $prebidPartner = $request->has('prebidPartner') ? $request->get('prebidPartner') : '';


        if (empty($dsp->company_name)) {
            $errors[] = 'Company name is required';
        }

        preg_match('/^[a-zA-Z0-9-_]+$/', $dsp->keyname, $matches);
        if (empty($dsp->keyname)) {
            $errors[] = 'Endpoint name is required';
        } elseif (empty($matches)) {
            $errors[] = 'Endpoint name should only contain letters, numbers and the underscore character';
        } elseif ((new SettingsDSP)->where('keyname', $dsp->keyname)->first()) {
            $errors[] = 'Endpoint name already exists';
        }

        if ($integration === 'prebid') {
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            if (empty($prebidPartner) || empty($prebidPartnerInfo)) {
                $errors[] = 'Partner is required';
            } else {
                $params = $prebidPartnerInfo->params;
                $prebidExtParams = [];
                $required = $params->required ?? [];
                foreach ($required as $param) {
                    if (empty($request->get($param))) {
                        $errors[] = ucfirst($param) . ' is required';
                    } elseif (isset($params->properties->$param->pattern)) {
                        preg_match('/' . $params->properties->$param->pattern . '/', $request->get($param), $matches);
                        if (empty($matches)) {
                            $errors[] = ucfirst($param) . ' is incorrect (pattern: ' . $params->properties->$param->pattern . ')';
                        }
                    } else {
                        $prebidExtParams[$param] = $request->get($param);
                    }
                }
            }

        }

        if ($integration === 'vast') {
            $dsp->at = 1;
            $dsp->adaptraffic = 0;
            $dsp->endpoint = $this->prepareVastEp($dsp->endpoint);
        } else {
            $dsp->endpoint = clearTrimHttp($dsp->endpoint);
        }

        if (empty($dsp->keyname)) {
            $errors[] = 'Endpoint name is required';
        }
        if (empty($dsp->endpoint)) {
            $errors[] = 'Endpoint is required';
        }
        if ($integration !== 'xml' && empty($formats)) {
            $errors[] = 'Formats is required';
        }
        if ($dsp->qps > 0 && $dsp->qps < 40) {
            $errors[] = 'QPS value must not be less than 40';
        }
        if (empty($dsp->maxbidfloor) && $integration === 'vast') {
            $errors[] = 'Bid Price is required';
        }
        if (empty($dsp->region)) {
            $errors[] = 'Region is required';
        }
        if (mb_strlen($dsp->endpoint) > 900) {
            $errors[] = 'Endpoint length must not be longer than 900 characters';
        }


        if (empty($errors)) {
            $listDsp = (new ListDSP);
            if (is_null($listDsp->where('company_name', $dsp->company_name)->first())) {
                $listDsp->company_name = $dsp->company_name;
                if (!$listDsp->save()) {
                    return response()->json([
                        'status' => 'error',
                        'errors' => ['Сompany creation error']
                    ], 400);
                }
            }

            foreach ($formats as $formatValue) {
                $dsp->$formatValue = 1;
            }
            $dsp->create_date = time();

            if ($dsp->save()) {
                if ($integration === 'prebid') {
                    $settingsDspPrebid = new SettingsDspPrebid();
                    $settingsDspPrebid->dspId = $dsp->id;
                    $settingsDspPrebid->ext = json_encode([
                        $prebidPartner => $prebidExtParams
                    ], JSON_FORCE_OBJECT);
                    $settingsDspPrebid->save();
                }

                return response()->json([
                    'status' => 'success',
                ]);
            } else {
                return response()->json([
                    'status' => 'error',
                    'errors' => ['Endpoint creation error']
                ], 400);
            }

        } else {
            return response()->json([
                'status' => 'error',
                'errors' => array_unique($errors)
            ], 400);
        }
    }

    private function prepareCompanyName($name)
    {
        $name = clean($name);
        $name = clearStringSpaceToLower($name);
        $name = str_replace('.', '_', $name);

        return $name;
    }

    private function prepareEpName($name, $region)
    {
        $name = $this->prepareCompanyName($name);
        if (empty($name) || empty($region)) {
            return $name;
        }

        if ($name) {
            if (session()->has('current_db') && session()->get('current_db') == "gothamads") {
                $name = 'GA' . '_' . $name . '_' . $region;
            } elseif (session()->has('current_db') && session()->get('current_db') == "bizzclick") {
                $name = 'BC' . '_' . $name . '_' . $region;
            } else {
                $name = $name . '_' . $region;
            }
        }

        return $name;
    }

    private function prepareVastEp($endpoint)
    {
        $parseUrl = parse_url($endpoint);
        if (!isset($parseUrl['query'])) {
            return $endpoint;
        }

        $macroses = SettingsDSP::VAST_MACROSES;
        if (app()->project->name === Project::SMARTY_ADS) {
            $macroses = $this->prepareMacrosSmartyStyle((new DspVastMacroses)->getMacroses());
        } elseif (app()->project->name === Project::GOTHAM_ADS) {
            $macroses = $this->prepareMacrosGothamStyle($macroses);
        }

        $params = explode('&', $parseUrl['query']);
        $countReplace = 0;
        $pubMacro = $this->array_partial_search($params, ['pid', 'pub'], ['appid', 'sspid', 'dpidsha', 'dpidmd']);
        foreach ($params as &$param) {
            $paramInfo = explode('=', $param);
            preg_match('/(\[|\{\{|\$\$)/', ($paramInfo[1] ?? ''), $matches);
            if (app()->project->name === Project::SMARTY_ADS && $pubMacro && $paramInfo[0] == 'schain') {
                $param = $paramInfo[0] . '=1.0,1!smartyads.com,' . $pubMacro . ',1';
                $countReplace++;
            } elseif (isset($macroses[$paramInfo[0]])) {
                $param = $paramInfo[0] . '=' . $macroses[$paramInfo[0]];
                $countReplace++;
            } elseif (!$matches) {
                $countReplace++;
            } elseif (!isset($macroses[$paramInfo[0]]) && $matches) {
                $this->notChangeMacroses[] = $paramInfo[0] . '=' . ($paramInfo[1] ?? '');
            }
        }

        unset($param);

        $endpoint = 'https://';
        $endpoint.= isset($parseUrl['host']) ? $parseUrl['host'] : '';
        $parseUrl['path'] = $parseUrl['path'] ?? '/';
        $endpoint.= $parseUrl['path'] . '?' . implode('&', $params);
        if (count($params) != $countReplace) {
            $this->changeAllMacroses = false;
        }
        return $endpoint;
    }

    private function prepareMacrosSmartyStyle($macroses)
    {
        $macroses = array_map(function($value) {
            if ($value === '[LOCATION_LAT]') {
                $value = '[LAT]';
            } elseif ($value === '[LOCATION_LON]') {
                $value = '[LON]';
            } elseif ($value === '[MINIMUM_DURATION]') {
                $value = '[MIN_DURATION]';
            } elseif ($value === '[MAXIMUM_DURATION]') {
                $value = '[MAX_DURATION]';
            } elseif ($value === '[CACHEBUSTER]') {
                $value = '[CB]';
            } else if ($value === '[SCHAIN]') {
                $value = "1.0,1!smartyads.com,{PUB_ID},1";
            } else if ($value === '[BUNDLE_ID]') {
                $value = '$$app_bundle$$';
            } else if ($value === '[INSERT_APPLICATION_BUNDLE]') {
                $value = '$$app_bundle$$';
            } else if ($value === '[INSERT_APPSTORE_URL]') {
                $value = '$$app_store_url$$';
            } else if ($value === '[INSERT_APPLICATION_NAME]') {
                $value = '$$app_name$$';
            } else if ($value === '[INSERT_DEVICE_ID]') {
                $value = '$$ifa$$';
            }

            $value = mb_strtolower($value);
            $value = str_replace('[', '$$', $value);
            $value = str_replace(']', '$$', $value);

            return $value;
        }, $macroses);

        return $macroses;
    }

    private function prepareMacrosGothamStyle($macroses)
    {
        $macroses = array_map(function($value) {
            if (in_array($value, ['[CATEGORY]','[DEVICE_CATEGORY]'])) {
                $value = '[IAB_CATEGORY]';
            }

            $value = str_replace('[', '{{', $value);
            $value = str_replace(']', '}}', $value);

            return $value;
        }, $macroses);

        return $macroses;
    }

    public function vastChangeMacros(Request $request)
    {
        $endpointUrl = $request->has('endpointUrl') ? clean($request->get('endpointUrl')) : '';
        $endpointUrl = $this->prepareVastEp($endpointUrl);
        $data['url'] = $endpointUrl;
        if (!$this->changeAllMacroses) {
            $data['warning'] = self::MACROSNOTCHANGED;
        }

        if (!empty($this->notChangeMacroses)) {
            $data['macrosesnotchange'] = $this->notChangeMacroses;
        }
        return json_encode($data);
    }

    public function array_partial_search($arrayToSearchIn, $arrayWhatToSearch, $arrayToExclude)
    {
        foreach ($arrayToSearchIn as $value) {
            $item = explode('=', $value);
            foreach ($arrayWhatToSearch as $keyword) {
                if (stripos($item[0], $keyword) !== false && strlen($item[1]) && !$this->in_array_case_insensitive($item[0], $arrayToExclude)) {
                    return $item[1];
                }
            }
        }

        return false;
    }

    public function in_array_case_insensitive($needle, $haystack) {
        return in_array(strtolower($needle), array_map('strtolower', $haystack));
    }

    /**
     * @return \Illuminate\Contracts\View\Factory|\Illuminate\View\View
     */
    public function editVue()
    {
        $title = 'DSP Edit';

        return view('vue', compact('title'));
    }

    public function getSettings($id): object
    {
        $dsp = SettingsDSP::find($id);
        if (is_null($dsp)) {
            return response() ->json([
                'success' => false,
                'errors' => ['Not Found']
            ], 404);
        }

        $prebid = $this->prepareSettingsPrebidInfo($dsp);

        if (app()->project->allowedFor(Project::ADMIX, Project::INTEGRAL_STREAM)) {
            $dsp->allowedRegions = $dsp->allowedRegions->pluck('region')->toArray();
        } else {
           $dsp->allowedRegions = [];
        }
        $dsp->trafquality = $this->convertJsonSettingsToArray($dsp->trafquality, false);
        $dsp->allowedCountries = $this->convertJsonSettingsToArray($dsp->allowedCountries);
        $dsp->devos = $this->convertJsonSettingsToArray($dsp->devos);
        $dsp->conntype = $this->convertJsonSettingsToArray($dsp->conntype);
        $dsp->allowedSSP = $this->convertJsonSettingsToArray($dsp->allowedSSP);
        $dsp->blocked_ssp = $this->convertJsonSettingsToArray($dsp->blocked_ssp);

        if ($dsp->company_name == 'bidswitch' && in_array(app()->project->name, [Project::SMARTY_ADS])) {
            $addData = SettingsDspAdditionalFields::find($id);
            if ($addData) {
                $dsp->wseat = $addData->wseat;
                $dsp->bseat = $addData->bseat;
            }
        }

        if (in_array(app()->project->name, [Project::SMARTY_ADS])) {
            $dsp->fraudPercPX = $dsp->fraudPercPX != 100 ? $dsp->fraudPercPX : 10;
            $defaultFraudPercPMNew = $dsp->fraudPercPM != 100 ? $dsp->fraudPercPM : 70;
            $dsp->fraudPercPM = $dsp->fraudPercPM != 100 ? $dsp->fraudPercPM : 20;
        }

        //====================================== Cookie Sync ===========================================================

        $userSyncPartners = [];
        $userSyncPartnerInternalId = 0;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
            $userSyncPartners = (new CookieSyncPartners)->getSyncPartners('dsp');
            $userSyncPartnerInternalId = (new CookieSyncPartnersMap)->getSyncPartnerInternalId('dsp_' . $id);
        }

        //======================================== Comments ============================================================

        $insideComment = null;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::ACEEX2, Project::INTEGRAL_STREAM])) {
            $insideComment = (new DspSspComments)->where([['id', $id], ['side', 'dsp']])->first();
        }

        return response()->json([
            'userRole' => Auth::user()->role,
            'dsp' => $dsp,
            'defaultFraudPercPMNew' => $defaultFraudPercPMNew ?? 0,
            'prebid' => $prebid,
            'secure' => getSecureTextStatus($dsp->secureprotocol),
            'userSync' => [
                'partners' => $userSyncPartners,
                'partnerInternalId' => $userSyncPartnerInternalId
            ],
            'insideComment' => $insideComment->comment ?? null,
            //'customInputs',
            'listCountries' => BaseModel::COUNTRIES,
            'listCountryGroups' => config('countries.groups'),
            'listSizes' => BaseModel::LIST_BANNER_SIZE,
            'allowedSizes' => array_values((new SettingsDSP)->getAllowedSize($id)),
            'listTrafficQualityType' => (new TrafficQualityTypes)->pluck('type', 'id')->toArray(),
            'arraySspTypes' => (new SspTrafficQuality)->getSspTypesByType(),
            'listSsp' => array_values((new SettingsSSP)->getListForDspSettings($dsp)),
            'quantityAllowedOrBlocked' => (new SettingsDSP)->getQuantityAllowedOrBlocked($id),
            'partnerApiLink' => (new ExternalApiLinksDSP)->find($id),
            'partnerApiLinkContentTypes' => ExternalApiLinksDSP::RESPONSE_CONTENT_TYPES,
        ]);
    }

    private function prepareSettingsPrebidInfo($dsp): array
    {
        $prebidInputs = [];
        $prebidPartnerInfo = [];
        $prebidSupportedFormats = [];
        if (!empty($dsp->usePrebid)) {
            $settingsDspPrebid = (new SettingsDspPrebid)->find($dsp->id);

            $prebidInputs = (array)$settingsDspPrebid->ext;
            $prebidPartner = array_keys($prebidInputs)[0];

            $prebidInputs = $prebidInputs[$prebidPartner];
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            $prebidSupportedFormats = $prebidPartnerInfo ? explode(',', $prebidPartnerInfo->supportedFormats) : '';
        }

        return [
            'inputs' => $prebidInputs,
            'info' => $prebidPartnerInfo,
            'formats' => $prebidSupportedFormats,
        ];
    }

    private function convertJsonSettingsToArray($jsonSettings, $onlyKeys = true): array
    {
        $arrSettings = json_decode($jsonSettings, true);
        if (!is_array($arrSettings) || !count($arrSettings)) {
            $arrSettings = [];
        }
        return $onlyKeys ? array_keys($arrSettings) : $arrSettings;
    }

    public function updateSettings(Request $request)
    {
        $errors = [];

        $dspId = (int)$request->id;
        $dspData = (new SettingsDSP)->find($dspId);

        if (is_null($dspData) || !_isManager(Auth::user()->role)) {
            return response() ->json([
                'success' => false,
                'errors' => ['Not Found']
            ], 404);
        }

        //=========================================== PREBID ===========================================================

        $prebidInputs = [];
        $prebidSupportedFormats = [];
        if (!empty($dspData['usePrebid'])) {
            $settingsDspPrebid = (new SettingsDspPrebid)->find($dspId);

            $prebidInputs = (array)$settingsDspPrebid->ext;
            $prebidPartner = array_keys($prebidInputs)[0];

            $prebidInputs = $prebidInputs[$prebidPartner];
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            $prebidSupportedFormats = $prebidPartnerInfo ? explode(',', $prebidPartnerInfo->supportedFormats) : '';
        }

        //==============================================================================================================

        $customInputs = [];
        if(in_array(app()->project->name, [Project::ACUITY]) && $dspData->company_name == 'rubicon'){
            $customInputs = (new RubiconCustomDspInputs)->find($dspId);
        }

        //==============================================================================================================

        if (!empty($request->post())) {

            $_POST = $request->post();

            //=========================================== PREBID ===========================================================

            if (!empty($dspData['usePrebid'])) {
                if (!empty($prebidPartnerInfo)) {
                    $params = $prebidPartnerInfo->params;
                    $prebidExtParams = [];
                    $required = $params->required ?? [];
                    foreach ($required as $param) {
                        if (empty($request->get($param))) {
                            $errors[] = ucfirst($param) . ' is required';
                        } elseif (isset($params->properties->$param->pattern)) {
                            preg_match('/' . $params->properties->$param->pattern . '/', $request->get($param), $matches);
                            if (empty($matches)) {
                                $errors[] = ucfirst($param) . ' is incorrect (pattern: ' . $params->properties->$param->pattern . ')';
                            }
                        } else {
                            $prebidExtParams[$param] = $request->get($param);
                        }
                    }
                }
            }

            //==============================================================================================================

            if (!empty($_POST['blocked_ssp']) && !empty($_POST['allowedSSP'])) {
                foreach ($_POST['blocked_ssp'] as $idEndpoint) {
                    if (in_array($idEndpoint, $_POST['allowedSSP'])) {
                        if ($_POST['allowedSSP'] === $this->convertJsonSettingsToArray($dspData->allowedSSP)) {
                            $errors[] = "Endpoint ID#$idEndpoint already exist in Allowed SSPs list";
                        } else {
                            $errors[] = "Endpoint ID#$idEndpoint already exist in Blocked SSPs list";
                        }
                    }
                }
            }

            foreach ($_POST as $p_key => $pval) {
                if (!is_array($pval)) {
                    $_POST[$p_key] = trim($_POST[$p_key]);
                }
            }

            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::ACEEX2, Project::INTEGRAL_STREAM])) {
                ///  inside comment
                $objComment = DspSspComments::find(['id' => $dspId, 'side' => 'dsp']);
                if ($request->has('insideComment') && !empty($request->insideComment)) {
                    if (empty($objComment)) {
                        $objComment = new DspSspComments();
                        $objComment->id = $dspId;
                        $objComment->side = 'dsp';
                    }
                    if ($objComment->comment !== $request->insideComment) {
                        (new BaseModel)->saveLog(
                            $dspId,
                            "\\App\\Models\\DspSspComments",
                            ['dsp_comment' => $objComment->comment],
                            ['dsp_comment' => $request->insideComment],
                            'update'
                        );
                        $objComment->comment = $request->insideComment;
                        $objComment->save();
                    }
                } else {
                    if ($objComment) {
                        (new BaseModel)->saveLog($dspId, "\\App\\Models\\DspSspComments", ['dsp_comment' => $objComment->comment], ['dsp_comment' => ''], 'update');
                        $objComment->delete();
                    }
                }
            }
            //pre($_POST);
            $was_changed = [];

            $simple_input = [
                'endpoint', 'qps', 'usebanner', 'usenative', 'usevideo', 'usepop', 'usexml', /*'nurl', 'email',*/ 'intstlonly', 'ctvonly', 'filterporn', 'tmax',
                'maxbidfloor', 'adaptraffic', 'reqdevid', 'reqcarrier', 'reqpubid', 'requserid', 'spendlimit', 'gzipResponses', 'goodtraffic',/* 'viewabilitytime', 'viewabilityperc', */
                'noMismatchedIpTraff', 'noMismatchedBundles', 'reqvideoapi', 'native_spec', 'desktop', 'inapp', 'mobweb', 'mraid', 'at', 'rewarded', 'marga', 'matchedUsersOnly',
                'fraudPercPM', 'allowVastRtb'
            ];
            if (in_array(app()->project->name, [Project::SMARTY_ADS])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::GOTHAM_ADS])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::BIZZ_CLICK])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::ACUITY])) {
                $simple_input[] = 'pixalate';
                $simple_input[] = 'donocheckmalware';
                $simple_input[] = 'latlon';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'suspendPercPM';
                $simple_input[] = 'gecpm';
                $simple_input[] = 'reqIfa';
                $simple_input[] = 'placementIdAppnexus';
                //$simple_input[] = 'gdpr';
                $simple_input[] = 'ccpa';
                $simple_input[] = 'tcf2';
            } elseif (in_array(app()->project->name, [Project::ACEEX, Project::ACEEX2])) {
                $simple_input[] = 'useaudio';
                $simple_input[] = 'minbidfloor';
                $simple_input[] = 'pixalate';
                $simple_input[] = 'upTraffic';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'suspendPercPM';
            } elseif (in_array(app()->project->name, [Project::ADVENUE])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'onlyFraud';
            }

            $simple_input[] = 'schain';
            if (in_array(app()->project->name, [Project::ACUITY])) {
                $simple_input[] = 'completeSchain';
            }

            /*if (!empty($dspData['useVast']) && empty($_POST['usevideo'])) {
                $_POST['usevideo'] = 1;
            }*/
            if ($_POST['qps'] > 0 && $_POST['qps'] < 40) {
                $errors[] = 'QPS value must not be less than 40';
            }
            if (mb_strlen($_POST['endpoint']) > 900) {
                $errors[] = 'Endpoint length must not be longer than 900 characters';
            }
            if (empty($_POST['maxbidfloor']) && !empty($dspData['useVast'])) {
                $errors[] = 'Bid Price is required';
            }
            if ((!empty($_POST['trafquality']['PXB']) || !empty($_POST['trafquality']['PXW'])) &&
                empty($_POST['fraudPercPX'])
            ) {
                $errors[] = 'PX Max IVT is required';
            }
            if ((!empty($_POST['trafquality']['PMB']) || !empty($_POST['trafquality']['PMW'])) &&
                empty($_POST['fraudPercPM'])
            ) {
                $errors[] = 'PM Max IVT is required';
            }
            foreach ($simple_input as $some_input) {
                if (!isset($_POST[$some_input])) {
                    $_POST[$some_input] = 0;
                }

                if ($some_input === 'endpoint' && !empty($dspData['useVast'])) {
                    $_POST[$some_input] = $this->prepareVastEp($_POST[$some_input]);
                }

                // ------------------ inversion ------------------

                if (in_array($some_input, ['donocheckmalware'])) {
                    if (empty($_POST[$some_input])) {
                        $_POST[$some_input] = 1;
                    } else {
                        $_POST[$some_input] = 0;
                    }
                }

                // -----------------------------------------------

                if (isset($dspData[$some_input]) && ($dspData[$some_input] != $_POST[$some_input])) {

                    if(key_exists($some_input, $this->preferentsByRole)){
                        if (!_isCustomManager(Auth::user()->role)) {
                          continue;
                        }
                    }
                    $was_changed[$some_input] = $dspData[$some_input] = $_POST[$some_input];
                }
            }

            $json_input = ['blocked_ssp', 'allowedSSP', 'allowedCountries', 'devos', 'conntype'];
            foreach ($json_input as $some_jsoninput) {
                if (!isset($_POST[$some_jsoninput])) {
                    $_POST[$some_jsoninput] = array();
                }
                sort($_POST[$some_jsoninput]);

                if ($dspData[$some_jsoninput] != $_POST[$some_jsoninput]) {
                    $temp_arr = array();
                    foreach ($_POST[$some_jsoninput] as $val) {
                        $temp_arr[$val] = true;
                    }
                    $was_changed[$some_jsoninput] = $dspData[$some_jsoninput] = json_encode($temp_arr, JSON_FORCE_OBJECT);
                }
            }

            // Secure protocol
            if (isset($_POST['secure'])) {
                if ($_POST['secure'] == 'secure') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'false', 1 => 'true'], JSON_FORCE_OBJECT);
                } elseif ($_POST['secure'] == 'nonsecure') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'true', 1 => 'false'], JSON_FORCE_OBJECT);
                } elseif ($_POST['secure'] == 'both') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'true', 1 => 'true'], JSON_FORCE_OBJECT);
                }
                if ($dspData['secureprotocol'] == $was_changed['secureprotocol']) unset($was_changed['secureprotocol']);
            }

            if ($_POST['trafquality'] !== json_decode($dspData['trafquality'], true)) {
                $was_changed['trafquality'] = $dspData['trafquality'] = json_encode($_POST['trafquality'], JSON_FORCE_OBJECT);
            }

            if (app()->project->allowedFor(Project::ADMIX, Project::INTEGRAL_STREAM)) {
                $errors = array_merge(
                    $errors,
                    app(AllowedRegionsService::class)->validateRequest($request)
                );
            }

            if (empty($errors)) {

                if (!empty($was_changed)) {
                    $dspData->update();
                }

                if (app()->project->allowedFor(Project::ADMIX, Project::INTEGRAL_STREAM)) {
                    $requestRegions = $request->get('allowedRegions', []);
                    app(AllowedRegionsService::class)->setFor($dspData, $requestRegions);
                }

                if (!empty($dspData['usePrebid'])) {
                    $settingsDspPrebid->ext = json_encode([
                        $prebidPartner => $prebidExtParams
                    ], JSON_FORCE_OBJECT);
                    $settingsDspPrebid->save();
                }

                if ($dspData['company_name'] == 'bidswitch' && in_array(app()->project->name, [Project::SMARTY_ADS])) {

                    if ($request->wseat == null && $request->bseat == null) {
                        $findId = SettingsDspAdditionalFields::find($dspId);
                        if ($findId) {
                            $findId->delete();
                        }
                    } else {
                        $addData = (new SettingsDspAdditionalFields)->firstOrNew(['dspId' => $dspId]);
                        $addData->wseat = $request->wseat;
                        $addData->bseat = $request->bseat;
                        $addData->save();
                    }
                }

                //////////////////////////////////////sizes
                $sizePost = !empty($_POST['allowedSizes']) ? $_POST['allowedSizes'] : [];
                $this->updateListBannerSize($dspId, $sizePost);

                /////////////////////////////////// Cookie Sync Update
                if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
                    if (empty($request->usersync_partner)) {
                        (new CookieSyncPartnersMap)->deleteSyncPartnersMapRelation('dsp', $dspId);
                    } else {
                        (new CookieSyncPartnersMap)->setUSyncPartner('dsp', $dspId, $request->usersync_partner);
                    }
                }

                //////////////////////////////////////ACUITY Custom Rubicon inputs
                if (in_array(app()->project->name, [Project::ACUITY]) && $dspData->company_name == 'rubicon') {
                    $newRow = (new RubiconCustomDspInputs)->firstOrNew(['dsp_id' => $dspId]);

                    $newRow->site_id = $_POST['site_id'] ?: null;
                    $newRow->zone_id = $_POST['zone_id'] ?: null;

                    if (is_null($newRow->site_id) && is_null($newRow->zone_id)) {
                        $newRow->delete();
                    } else {
                        $newRow->save();
                    }


                }

                return response()->json([
                    'success' => true,
                    'message' => 'Endpoint is saved!'
                ]);
            }

        }

        return response() ->json([
            'success' => false,
            'errors' => $errors
        ], 400);
    }

    public function edit(Request $request)
    {
        $errors = [];

        $dspId = (int)$request->id;
        $dspData = (new SettingsDSP)->find($dspId);

        if (is_null($dspData)) {
            return abort(404);
        }

        if (!_isManager(Auth::user()->role)) {
            return abort(404);
        }

        $refererCompanyUrl = $request->has('back') && $request->get('back') === 'company';

        //=========================================== PREBID ===========================================================

        $prebidInputs = [];
        $prebidSupportedFormats = [];
        if (!empty($dspData['usePrebid'])) {
            $settingsDspPrebid = (new SettingsDspPrebid)->find($dspId);

            $prebidInputs = (array)$settingsDspPrebid->ext;
            $prebidPartner = array_keys($prebidInputs)[0];

            $prebidInputs = $prebidInputs[$prebidPartner];
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            $prebidSupportedFormats = $prebidPartnerInfo ? explode(',', $prebidPartnerInfo->supportedFormats) : '';
        }

        //==============================================================================================================

        $listBannerSize = BaseModel::LIST_BANNER_SIZE;
        //sort($listBannerSize);
        //$listRegions = (new Servers)->getAllRegions();
        $listCountries = BaseModel::COUNTRIES;
        $countryGroups = json_encode(config('countries.groups'));
        $listDeviceOS = BaseModel::LIST_DEVICE_OS;
        $listConTypes = BaseModel::LIST_CON_TYPES;
        $listMraid = BaseModel::LIST_MRAID;

        $allowedSize = (new SettingsDSP)->getAllowedSize($dspId);
        $listSsp = (new SettingsSSP)->select('id', 'region')
            ->selectRaw("concat(statname, ' (ID#', id, ')') as name ")
            ->where('active', '<>', SettingsSSP::STATUS_ARCHIVE)
            ->orderBy('name')
            ->get()->keyBy('id')->toArray();


        $listTrafficQualityType = json_encode((new TrafficQualityTypes)->pluck('type', 'id')->toArray());
        $arraySspTypes = (new SspTrafficQuality)->getSspTypes();

        $quantityAllowedOrBlocked = (new SettingsDSP)->getQuantityAllowedOrBlocked($dspId);

        $customInputs = [];
        if(in_array(app()->project->name, [Project::ACUITY]) && $dspData->company_name == 'rubicon'){
            $customInputs = (new RubiconCustomDspInputs)->find($dspId);
        }

        $blocked_ssp = [];
        if (!empty($dspData['blocked_ssp'])) {
            $arr_SSP_ids = json_decode($dspData['blocked_ssp'], true);
            if (is_array($arr_SSP_ids) && count($arr_SSP_ids) > 0) {
                $blocked_ssp = array_keys($arr_SSP_ids);
            }
        }

        $allowedSSP = [];
        if (!empty($dspData['allowedSSP'])) {
            $arr_SSP_ids = json_decode($dspData['allowedSSP'], true);
            if (is_array($arr_SSP_ids) && count($arr_SSP_ids) > 0) {
                $allowedSSP = array_keys($arr_SSP_ids);
            }
        }

        $allowedCountries = [];
        if (!empty($dspData['allowedCountries'])) {
            $arr_allowedCountries = json_decode($dspData['allowedCountries'], true);
            if (is_array($arr_allowedCountries) && count($arr_allowedCountries) > 0) {
                $allowedCountries = array_keys($arr_allowedCountries);
            }
        }

        $devos = (!empty($dspData['devos']) && ($dspData['devos'] !== '{}')) ? array_keys(json_decode($dspData['devos'], true)) : [];
        $conntype = (!empty($dspData['conntype']) && ($dspData['conntype'] !== '{}')) ? array_keys(json_decode($dspData['conntype'], true)) : [];

        $secure = getSecureTextStatus($dspData['secureprotocol']) ?: '';

        $trafquality = isset($dspData['trafquality']) ? json_decode($dspData['trafquality'], true) : [];
        $trafficQuality = [];
        $trafficQuality['PMB'] = (isset($trafquality['PMB']) && $trafquality['PMB'] == 'true') ? 1 : 0;
        $trafficQuality['PMW'] = (isset($trafquality['PMW']) && $trafquality['PMW'] == 'true') ? 1 : 0;
        //$trafficQuality['Udger'] = (isset($trafquality['Udger']) && $trafquality['Udger'] == 'true') ? 1 : 0;
        //$trafficQuality['FB'] = (isset($trafquality['FB']) && $trafquality['FB'] == 'true') ? 1 : 0;
        //$trafficQuality['FW'] = (isset($trafquality['FW']) && $trafquality['FW'] == 'true') ? 1 : 0;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ACUITY, Project::ACEEX, Project::ACEEX2])) {
            $trafficQuality['PXB'] = (isset($trafquality['PXB']) && $trafquality['PXB'] == 'true') ? 1 : 0;
            $trafficQuality['PXW'] = (isset($trafquality['PXW']) && $trafquality['PXW'] == 'true') ? 1 : 0;
        }

        //====================================== Cookie Sync ===========================================================

        $userSyncPartners = [];
        $userSyncPartnerInternalId = 0;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
            $region = str_replace('US_', '', $dspData['region']);
            $userSyncPartners = (new CookieSyncPartners)->getSyncPartners('dsp');
            $userSyncPartnerInternalId = (new CookieSyncPartnersMap)->getSyncPartnerInternalId('dsp_' . $dspId);
        }

        //==============================================================================================================

        $insideComment = null;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::ACEEX2])) {
            $insideComment = (new DspSspComments)->where([['id', $dspId], ['side', 'dsp']])->first();
            $insideComment = $insideComment->comment ?? null;
        }

        if (!empty($_POST)) {

            //=========================================== PREBID ===========================================================

            if (!empty($dspData['usePrebid'])) {
                if (!empty($prebidPartnerInfo)) {
                    $params = $prebidPartnerInfo->params;
                    $prebidExtParams = [];
                    $required = $params->required ?? [];
                    foreach ($required as $param) {
                        if (empty($request->get($param))) {
                            $errors[] = ucfirst($param) . ' is required';
                        } elseif (isset($params->properties->$param->pattern)) {
                            preg_match('/' . $params->properties->$param->pattern . '/', $request->get($param), $matches);
                            if (empty($matches)) {
                                $errors[] = ucfirst($param) . ' is incorrect (pattern: ' . $params->properties->$param->pattern . ')';
                            }
                        } else {
                            $prebidExtParams[$param] = $request->get($param);
                        }
                    }
                }

            }

            //==============================================================================================================

            //если выбраны все страны, это равносильно что не выбрано ни одной. Что бы не засерать базу ещё больше, чистим.
            if (!empty($_POST['allowedCountries']) && count($_POST['allowedCountries']) == count($listCountries)) {
                $_POST['allowedCountries'] = [];
            }

            if (!empty($_POST['allowedCountries'])) {
                foreach ($_POST['allowedCountries'] as $key => $val) {
                    if (!key_exists($val, $listCountries)) {
                        unset($_POST['allowedCountries'][$key]);
                    }
                }
            }

            if (!empty($_POST['allowedSSP'])) {
                foreach ($_POST['allowedSSP'] as $key => $idEndpoint) {
                    if (!key_exists($idEndpoint, $listSsp)) {
                        unset($_POST['allowedSSP'][$key]);
                    }
                }
            }
            if (!empty($_POST['blocked_ssp'])) {
                foreach ($_POST['blocked_ssp'] as $key => $idEndpoint) {
                    if (!key_exists($idEndpoint, $listSsp)) {
                        unset($_POST['blocked_ssp'][$key]);
                    }
                }
            }

            if (!empty($_POST['blocked_ssp']) && !empty($_POST['allowedSSP'])) {
                foreach ($_POST['blocked_ssp'] as $idEndpoint) {
                    if (in_array($idEndpoint, $_POST['allowedSSP'])) {
                        if ($_POST['allowedSSP'] === $this->convertJsonSettingsToArray($dspData->allowedSSP)) {
                            $errors[] = "Endpoint ID#$idEndpoint already exist in Allowed SSPs list";
                        } else {
                            $errors[] = "Endpoint ID#$idEndpoint already exist in Blocked SSPs list";
                        }
                    }
                }
            }

            foreach ($_POST as $p_key => $pval) {
                if (!is_array($pval)) {
                    $_POST[$p_key] = trim($_POST[$p_key]);
                }
            }

            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::ACEEX2])) {
                ///  inside comment
                if ($request->has('insideComment') && !empty($request->insideComment)) {
                    $objComment = (new DspSspComments)->where([['id', $dspId], ['side', 'dsp']])->first();
                    if (empty($objComment)) {
                        $objComment = new DspSspComments();
                        $objComment->id = $dspId;
                        $objComment->side = 'dsp';
                    }
                    $objComment->comment = $request->insideComment;
                    $objComment->save();
                } else {
                    (new DspSspComments)->where([['id', $dspId], ['side', 'dsp']])->delete();
                }
            }
            //pre($_POST);
            $was_changed = [];

            $simple_input = [
                'endpoint', 'qps', 'usebanner', 'usenative', 'usevideo', 'usepop', 'usexml', /*'nurl', 'email',*/ 'intstlonly', 'ctvonly', 'filterporn', 'tmax',
                'maxbidfloor', 'adaptraffic', 'reqdevid', 'reqcarrier', 'reqpubid', 'requserid', 'spendlimit', 'gzipResponses', 'goodtraffic',/* 'viewabilitytime', 'viewabilityperc', */
                'noMismatchedIpTraff', 'noMismatchedBundles', 'reqvideoapi', 'native_spec', 'desktop', 'inapp', 'mobweb', 'mraid', 'at', 'rewarded', 'marga', 'matchedUsersOnly',
                'fraudPercPM', 'allowVastRtb'
            ];
            if (in_array(app()->project->name, [Project::SMARTY_ADS])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::GOTHAM_ADS])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::BIZZ_CLICK])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'useaudio';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'fraudPercPM';
                $simple_input[] = 'pxIpBlack';
                $simple_input[] = 'pxIfaBlack';
                $simple_input[] = 'tcf2';
                $simple_input[] = 'coppa';
            } elseif (in_array(app()->project->name, [Project::ACUITY])) {
                $simple_input[] = 'pixalate';
                $simple_input[] = 'donocheckmalware';
                $simple_input[] = 'latlon';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'suspendPercPM';
                $simple_input[] = 'gecpm';
                $simple_input[] = 'reqIfa';
                $simple_input[] = 'placementIdAppnexus';
                //$simple_input[] = 'gdpr';
                $simple_input[] = 'ccpa';
                $simple_input[] = 'tcf2';
            } elseif (in_array(app()->project->name, [Project::ACEEX, Project::ACEEX2])) {
                $simple_input[] = 'useaudio';
                $simple_input[] = 'minbidfloor';
                $simple_input[] = 'pixalate';
                $simple_input[] = 'upTraffic';
                $simple_input[] = 'fraudPercPX';
                $simple_input[] = 'suspendPercPM';
            } elseif (in_array(app()->project->name, [Project::ADVENUE])) {
                $simple_input[] = 'ctvExclude';
                $simple_input[] = 'onlyFraud';
            }

            $simple_input[] = 'schain';
            if (in_array(app()->project->name, [Project::ACUITY])) {
                $simple_input[] = 'completeSchain';
            }

            if (!empty($dspData['useVast']) && empty($_POST['usevideo'])) {
                $_POST['usevideo'] = 1;
            }
            if ($_POST['qps'] > 0 && $_POST['qps'] < 40) {
                $errors[] = 'QPS value must not be less than 40';
            }
            if (mb_strlen($_POST['endpoint']) > 900) {
                $errors[] = 'Endpoint length must not be longer than 900 characters';
            }
            if (empty($_POST['maxbidfloor']) && !empty($dspData['useVast'])) {
                $errors[] = 'Bid Price is required';
            }

            foreach ($simple_input as $some_input) {
                if (!isset($_POST[$some_input])) {
                    $_POST[$some_input] = 0;
                }

                if ($some_input === 'endpoint' && !empty($dspData['useVast'])) {
                    $_POST[$some_input] = $this->prepareVastEp($_POST[$some_input]);
                }

                // ------------------ inversion ------------------

                if (in_array($some_input, ['donocheckmalware'])) {
                    if (empty($_POST[$some_input])) {
                        $_POST[$some_input] = 1;
                    } else {
                        $_POST[$some_input] = 0;
                    }
                }

                // -----------------------------------------------

                if (isset($dspData[$some_input]) && ($dspData[$some_input] != $_POST[$some_input])) {

                    if(key_exists($some_input, $this->preferentsByRole)){
                        if (!_isCustomManager(Auth::user()->role)) {
                            continue;
                        }
                    }
                    $was_changed[$some_input] = $dspData[$some_input] = $_POST[$some_input];
                }
            }

            $json_input = ['blocked_ssp', 'allowedSSP', 'allowedCountries', 'devos', 'conntype'];
            foreach ($json_input as $some_jsoninput) {
                if (!isset($_POST[$some_jsoninput])) {
                    $_POST[$some_jsoninput] = array();
                }
                sort($_POST[$some_jsoninput]);

                if ($dspData[$some_jsoninput] != $_POST[$some_jsoninput]) {
                    $temp_arr = array();
                    foreach ($_POST[$some_jsoninput] as $val) {
                        $temp_arr[$val] = true;
                    }
                    $was_changed[$some_jsoninput] = $dspData[$some_jsoninput] = json_encode($temp_arr, JSON_FORCE_OBJECT);
                }
            }

            // Secure protocol
            if (isset($_POST['secure'])) {
                if ($_POST['secure'] == 'secure') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'false', 1 => 'true'], JSON_FORCE_OBJECT);
                } elseif ($_POST['secure'] == 'nonsecure') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'true', 1 => 'false'], JSON_FORCE_OBJECT);
                } elseif ($_POST['secure'] == 'both') {
                    $was_changed['secureprotocol'] = $dspData['secureprotocol'] = json_encode([0 => 'true', 1 => 'true'], JSON_FORCE_OBJECT);
                }
                if ($dspData['secureprotocol'] == $was_changed['secureprotocol']) unset($was_changed['secureprotocol']);
            }

            $post_trafquality = [];
            $post_trafquality['PMB'] = isset($_POST['trafqualityPMB']) ? 1 : 0;
            $post_trafquality['PMW'] = isset($_POST['trafqualityPMW']) ? 1 : 0;
            //$post_trafquality['Udger'] = isset($_POST['trafqualityUdger']) ? 1 : 0;
            //$post_trafquality['FB'] = isset($_POST['trafqualityFB']) ? 1 : 0;
            //$post_trafquality['FW'] = isset($_POST['trafqualityFW']) ? 1 : 0;
            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ACUITY, Project::ACEEX, Project::ACEEX2])) {
                $tmpError = false;
                if (
                    $request->has('trafqualityPXB') &&
                    $request->has('trafqualityPXW')
                ) {
                    $tmpError = true;
                }

                if ($tmpError) {
                    $post_trafquality['PXB'] = 0;
                    $post_trafquality['PXW'] = 0;
                } else {
                    $post_trafquality['PXB'] = isset($_POST['trafqualityPXB']) ? 1 : 0;
                    $post_trafquality['PXW'] = isset($_POST['trafqualityPXW']) ? 1 : 0;
                }
            }
            if ($post_trafquality != $trafficQuality) {
                if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ACUITY, Project::ACEEX, Project::ACEEX2])) {
                    $trafqualityArr = [
                        'PMB' => (bool) $post_trafquality['PMB'],
                        'PMW' => (bool) $post_trafquality['PMW'],
                        //'Udger' => (bool) $post_trafquality['Udger'],
                        //'FB' => (bool) $post_trafquality['FB'],
                        //'FW' => (bool) $post_trafquality['FW'],
                        'PXB' => (bool) $post_trafquality['PXB'],
                        'PXW' => (bool) $post_trafquality['PXW']
                    ];
                } else {
                    $trafqualityArr = [
                        'PMB' => (bool)$post_trafquality['PMB'],
                        'PMW' => (bool)$post_trafquality['PMW'],
                        //'Udger' => (bool) $post_trafquality['Udger'],
                        //'FB' => (bool)$post_trafquality['FB'],
                        //'FW' => (bool)$post_trafquality['FW']
                    ];
                }
                $was_changed['trafquality'] = $dspData['trafquality'] = json_encode($trafqualityArr, JSON_FORCE_OBJECT);
            }


            if (empty($errors)) {

                if (!empty($was_changed)) {
                    $dspData->update();
                }

                if (!empty($dspData['usePrebid'])) {
                    $settingsDspPrebid->ext = json_encode([
                        $prebidPartner => $prebidExtParams
                    ], JSON_FORCE_OBJECT);
                    $settingsDspPrebid->save();
                }

                //////////////////////////////////////sizes
                // размеры в базе сейчас
                $sizesNow = $allowedSize;
                // Размеры из поста
                $sizePost = !empty($_POST['AllowedSize']) ? $_POST['AllowedSize'] : [];
                $this->updateListBannerSize($dspId, $sizePost);

                /////////////////////////////////// Cookie Sync Update
                if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
                    if (empty($request->usersync_partner)) {
                        (new CookieSyncPartnersMap)->deleteSyncPartnersMapRelation('dsp', $dspId);
                    } else {
                        (new CookieSyncPartnersMap)->setUSyncPartner('dsp', $dspId, $request->usersync_partner);
                    }
                }

                //////////////////////////////////////ACUITY Custom Rubicon inputs
                if (in_array(app()->project->name, [Project::ACUITY]) && $dspData->company_name == 'rubicon') {
                    $newRow = (new RubiconCustomDspInputs)->firstOrNew(['dsp_id' => $dspId]);

                    $newRow->site_id = $_POST['site_id'] ?: null;
                    $newRow->zone_id = $_POST['zone_id'] ?: null;

                    if (is_null($newRow->site_id) && is_null($newRow->zone_id)) {
                        $newRow->delete();
                    } else {
                        $newRow->save();
                    }


                }

                return redirect('dsp/edit/id/' . $dspId . ($refererCompanyUrl ? '?back=company' : ''));
            }

        }

        return view('dsp.edit', compact(
                'errors',
                'dspData',
                'allowedSize',
                'listSsp',
                'listTrafficQualityType',
                'listBannerSize',
                'listCountries',
                'countryGroups',
                'listDeviceOS',
                'listConTypes',
                'quantityAllowedOrBlocked',
                'blocked_ssp',
                'allowedSSP',
                'allowedCountries',
                'devos',
                'conntype',
                'secure',
                'listMraid',
                'trafficQuality',
                'userSyncPartners',
                'userSyncPartnerInternalId',
                'insideComment',
                'customInputs',
                'arraySspTypes',
                'refererCompanyUrl',
                'prebidSupportedFormats',
                'prebidInputs'
            )
        );
    }

    private function updateListBannerSize($idDsp, array $tempArraySizes)
    {
        $nowArraySize = (new SettingsDSP)->getAllowedSize($idDsp);

        $delArray = array_diff($nowArraySize, $tempArraySizes);
        $addArray = array_diff($tempArraySizes, $nowArraySize);

        if (!empty($delArray) || !empty($addArray)) {

            if ($delArray && count($delArray) > 0) {
                foreach ($delArray as $value) {
                    if (!empty($value)) {
                        $arraySize = explode("x", $value);
                        (new AllowedSize)->where([
                            ['dsp_id', '=', $idDsp],
                            ['width', '=', $arraySize[0]],
                            ['height', '=', $arraySize[1]]
                        ])->delete();
                    }
                }
                (new BaseModel)->saveLog($idDsp, "\\App\\Models\\AllowedSize", array_values($delArray), [], 'delete');
            }

            if (count($addArray) > 0) {
                foreach ($addArray as $value) {
                    if (!empty($value)) {
                        $arraySize = explode("x", $value);
                        if (!is_numeric($arraySize[0]) || !is_numeric($arraySize[1])) {
                            continue;
                        }
                        (new AllowedSize)->insert([
                            ['dsp_id' => $idDsp, 'width' => $arraySize[0], 'height' => $arraySize[1]],
                        ]);
                    }
                }
                (new BaseModel)->saveLog($idDsp, "\\App\\Models\\AllowedSize", [], array_values($addArray), 'create');
            }

        }
    }


    public function loadGraphDspSpend(Request $request)
    {
        if (!empty($_POST['dsp_id'])) {
            $DSPData = (new SettingsDSP)->find($_POST['dsp_id']);
            $time_week_ago = mktime(0, 0, 0) - 518400;
            $DSPwin = (new StatImpression)->getSpendDsp($DSPData['keyname'], $time_week_ago);
            if ($DSPwin) {
                $week = array();
                for ($i = 6; $i >= 0; $i--) {
                    $day = date('Y-m-d', time() - ($i * 24 * 3600));
                    $arr = array(
                        "day" => $day,
                        "win" => isset($DSPwin[$day]) ? $DSPwin[$day] : 0,
                    );
                    $week[] = $arr;
                }
                //Create Table
                $table = '<thead><tr>';
                foreach ($week as $one_day) {
                    $table .= '<th>' . $one_day['day'] . '</th>';
                }
                $table .= '</tr></thead>';
                $table .= '<tbody>';
                $table .= '<tr data-axis="0" data-name="Spend $ " data-visible="true" data-y="spend">';
                foreach ($week as $one_day) {
                    $table .= '<td>' . $one_day['win'] . '</td>';
                }
                $table .= '</tr></tbody>';
                echo $table;
            } else {
                echo '';
            }
        } else {
            echo '';
        }
    }

    public function companyListExport(ListDSP $listDsp)
    {
        $fileName = 'skad-network-ids.csv';
        $list = $listDsp->getListCompany();

        $headers = array(
            "Content-type"        => "text/csv",
            "Content-Disposition" => "attachment; filename=$fileName",
            "Pragma"              => "no-cache",
            "Cache-Control"       => "must-revalidate, post-check=0, pre-check=0",
            "Expires"             => "0"
        );

        $columns = array('skad_network_id');

        $callback = function() use($list, $columns) {
            $file = fopen('php://output', 'w');
            //fputcsv($file, $columns);

            foreach ($list as $item) {
                if (!$item['scadNetworkId']) {
                    continue;
                }

                $row['skad_network_id']  = $item['scadNetworkId'];

                $pattern = '/(,+|\n+)/';
                $row['skad_network_id'] = preg_replace($pattern, ' ', $row['skad_network_id']);
                $pattern = '/\s\s+/';
                $row['skad_network_id'] = preg_replace($pattern, ' ', $row['skad_network_id']);
                $row['skad_network_id'] = str_replace(" ", "\n", $row['skad_network_id']);
                fwrite($file, implode("\n", $row)."\n");
            }

            fclose($file);
        };

        return response()->stream($callback, 200, $headers);
    }

    public function companyList(ListDSP $listDsp)
    {
        return response()->json($listDsp->getListCompany());
    }

    public function companyActiveList(ListDSP $listDsp)
    {

        $arrayCampaigns = $listDsp->getListActiveCompany();
        $arrayReturn = [];

        if (!empty($arrayCampaigns)) {
            foreach ($arrayCampaigns as $id => $name) {
                $arrayReturn[] = ['name' => $name, 'id' => $id];
            }
        }
        return response()->json($arrayReturn);
    }

    public function getListDspByRegion(Request $request)
    {
        $id = $request->id;
        $selectedRegion = $request->region;
        $isVast = $request->isVast;
        $isPrebid = $request->isPrebid;
        $isPushXML = $request->isPushXML;

        $listDsp = SettingsDSP::selectRaw("id, concat(keyname, ' (ID#', id, ')') as name, company_name")
            ->where('active', '=', 1)
            ->where('usexml', '=', 0)
            ->where(function ($qr) use ($selectedRegion) {
                $qr->where('region', '=', $selectedRegion)
                    ->orWhere('region', '=', NULL);
                });
        if ($isVast==0 && $isPrebid==0 && $isPushXML==0) {
            $listDsp->where(function ($q) {
                $q->where('usevast', '=', 0)
                    ->orWhere(function ($query) {
                        $query->where("usevast", "=", 1)
                            ->where('allowVastRtb', "=", 1);
                        });
                });
        }
        $listDsp = $listDsp->orderBy('name')
        ->get()->toArray();

        return response()->json($listDsp);
    }

    public function companyLimits(ListDSP $listDsp)
    {
        return response()->json($this->prepareCompanyLimits($listDsp->getCompanyLimits()));
    }


    private function prepareCompanyLimits($arrayDsp){

        $arrayActive = (new ListDSP)->getActive();

        $hasHistoryBalance = (new BalanceHistory)->distinct('cid')->pluck('cid')->toArray();


        foreach ($arrayDsp as &$company){
            $company['history'] = 0;
            if (in_array($company['id'], $hasHistoryBalance)) {
                $company['history'] = 1;
            }

            if (!empty($company['reached'])) {
                $company['status'] = 'Cap Reached';
            } else {
                if(key_exists($company['id'], $arrayActive)){
                    $company['status'] = 'Active';
                }else {
                    $company['status'] = 'Inactive';
                }
            }
        }

        return $arrayDsp;

    }

    public function setCompanyLimits(Request $request)
    {
        $companyId = $request->has('dsp') && !empty((int)$request->dsp) ? (int)$request->dsp : false;
        $limit = $request->has('limit') && !empty((int)$request->limit) ? (int)$request->limit : 0;


        if (empty($companyId)) {
            return abort(404);
        }

        if (Auth::user()->role != 'finance') {
            return abort(404);
        }

        $dspData = (new ListDSP)->find($companyId);

        if ($dspData) {
            $dspData->prepay_cap = $limit;
            $dspData->save();
        } else {
            return abort(404);
        }
        return  response()->json($dspData->toArray());

    }

    public function listDsp(SettingsDSP $settingsDsp, Request $request)
    {
        $listDsp = $settingsDsp->getList(null, $request->search);

        if (empty($listDsp)) {
            return response()->json([
                'name' => [],
                'statname' => [],
                'region' => ['US_EAST']
            ]);
        }
        if (app()->project->allowedFor(Project::ACEEX)) {
            $listDsp['region'][] = 'SGP';
            $listDsp['region'][] = 'EU';
            $listDsp['region'] = array_values(array_filter(array_unique($listDsp['region'])));
        }
        $listDsp['statname'] = array_values($listDsp['statname']);

        return response()->json($listDsp);
    }

    public function listDspEpById(SettingsDSP $settingsDsp, Request $request)
    {
        $listDsp = $settingsDsp->getList(null, $request->search);

        if (empty($listDsp)) {
            return response()->json([]);
        }
        return response()->json($listDsp['company_statname']);
    }

    public function listPrebidPartners(PrebidPartners $prebidPartners)
    {
        return response()->json($prebidPartners->allPartners());
    }

    public function bidRequest(Request $request)
    {
        $dspId = (int)$request->id;
        $dspData = (new SettingsDSP)->find($dspId);

        if (is_null($dspData)) {
            return abort(404);
        }

        $examples = (new RequestExamples)->getExamplesByDsp($dspId);
        if ($examples) {
            $str_result_action = json_decode(base64_decode($examples->data));
            $dateUpdate = date('Y-m-d H:i', strtotime($examples->lastUpdate));
        } else {
            $str_result_action = [];
        }

        return view('dsp.bidRequest', [
            'dspData' => $dspData,
            'dateUpdate' => $dateUpdate ?? false,
            'content' => $str_result_action
        ]);
    }

    public function bidResponse(Request $request)
    {
        $dspId = (int)$request->id;
        $dspData = (new SettingsDSP)->find($dspId);

        if (is_null($dspData)) {
            return abort(404);
        }

        $examples = (new ResponseExamples)->getExamplesByDsp($dspId);
        if ($examples) {
            $str_result_action = json_decode(base64_decode($examples->data));
            $dateUpdate = date('Y-m-d H:i', strtotime($examples->lastUpdate));
        } else {
            $str_result_action = [];
        }

        return view('dsp.bidResponse', [
            'dspData' => $dspData,
            'dateUpdate' => $dateUpdate ?? false,
            'content' => $str_result_action
        ]);
    }


    //================================= Lists =======================================

    /**
     * @return \Illuminate\Contracts\View\Factory|\Illuminate\View\View
     */
    public function blockedCridsVue(Request $request)
    {
        $title = 'Blocked Crids';

        return view('vue', compact('title'));
    }

    public function blockedCrids(Request $request)
    {
        $orderBy = $request->has('orderBy') ? $request->orderBy : 'crid';
        $sortBy = $request->has('sortBy') ? $request->sortBy : 'desc';
        $errorCodes = [
            0 => 'was added manually',
            1 => 'parse error',
            2 => 'unclosed iframe tag'
        ];

        $settingsDspTblName = (new SettingsDSP)->getTable();
        $settingsDspPKName = (new SettingsDSP)->getKeyName();
        $brokenCridsTblName = (new BrokenCrids)->getTable();
        $blockedCrids = (new BrokenCrids)->select([
            "$settingsDspTblName.keyname",
            "$brokenCridsTblName.dsp_id",
            "$brokenCridsTblName.crid",
            "$brokenCridsTblName.ourcrid",
            "$brokenCridsTblName.error_code",
        ])
            ->join($settingsDspTblName, "$brokenCridsTblName.dsp_id", '=', "$settingsDspTblName.$settingsDspPKName")
            ->orderBy($orderBy, $sortBy)
            ->paginate(1000);

        $listDsp = (new SettingsDSP)->getList();

        return response()->json([
            'errorCodes' => $errorCodes,
            'blockedCrids' => $blockedCrids,
            'listDsp' => $listDsp
        ]);
    }

    public function loadAdmExample(Request $request)
    {
        $ourcrid = (string)$request->ourcrid;

        if (!empty($ourcrid)) {
            $adm = (new Adm)->where('our_crid', $ourcrid)->first();
            if ($adm) {
                echo $adm['adm'];
            } else {
                echo 'no adm';
            }
        } else {
            echo 'no adm';
        }
    }

    private function addBlockedCrid(Request $request)
    {
        if (!$request->has('dspid') || !$request->has('crid')) {
            return response('Not Found', 404);
        }

        $dspid = (int)$request->dspid;
        $crid = clean($request->crid);

        if (!$dspid || !$crid) {
            die('Empty data');
        }

        $brokenCrid = new BrokenCrids();
        $brokenCrid->dsp_id = $dspid;
        $brokenCrid->crid = $crid;
        if ($brokenCrid->save()) {
            echo 'ok';
        } else {
            echo 'Error insert';
        }
    }

    private function deleteBlockedCrid(Request $request)
    {
        if (!$request->has('dspid') || !$request->has('crid')) {
            return response('Not Found', 404);
        }

        $dspid = (int)$request->dspid;
        $crid = clean($request->crid);

        if (!$dspid || !$crid) {
            die('Empty data');
        }

        $res = (new BrokenCrids)->where([
            ['dsp_id', '=', $dspid],
            ['crid', '=', $crid]
        ])->delete();

        if ($res) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    //============================== Ajax methods ===================================

    private function getExamples(Request $request)
    {
        if (empty($request->dspId) || empty($request->date) || empty($request->type) || empty($request->error) || !in_array($request->type, ['request', 'response'])) {
            die('');
        }

        $dspId[] = intval($request->dspId);
        $date = clean($request->date);
        $error = clean($request->error);
        $type = clean($request->type);

        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX])) {
            $dspInfoExamples = (new DspInfoStats)->getDspInfoExamples($date, $dspId);
        } else {
            $dspInfoExamples = $this->getDspHourlyStatsExamples($date, $dspId);
        }

        $output = '';
        $i = 0;
        if (!empty($dspInfoExamples[$dspId[0]][$date][$error]['examples'])) {
            foreach ($dspInfoExamples[$dspId[0]][$date][$error]['examples'] as $example) {
                $i++;
                $tmpArr = json_decode($example, 1);
                if (empty($tmpArr)) continue;
                $request_response = $tmpArr[$type];
                $class = ($i % 2) == 1 ? 'note-success' : 'note-warning';
                $output .= "<div class=\"note $class\"><pre>";
                $final = json_decode(gzuncompress(base64_decode($request_response)), true);
                if (is_string($final)) {
                    $output .= print_r(htmlspecialchars(indent($final)), 1);
                } else {
                    $output .= print_r(htmlspecialchars(indent(json_encode($final, JSON_UNESCAPED_SLASHES))), 1);
                }
                $output .= '</pre></div>';
            }
        }
        die($output);
    }

    public function saveEndpointVue(Request $request)
    {
        $endpoint = clean($request->get('endpoint'));
        $dspSettings = SettingsDSP::findOrFail($request->get('id'));

        $dspSettings->update([
            'endpoint'=> $dspSettings->useVast ? $endpoint : clearTrimHttp($endpoint)
        ]);

        return response()->json([
            'status' => 'success',
        ]);
    }

    public function saveQpsVue(Request $request)
    {
        SettingsDSP::findOrFail($request->get('id'))->update([
            'qps'=> $request->get('qps'),
        ]);

        return response()->json([
            'status' => 'success',
        ]);
    }

    public function changeSpendLimitVue(Request $request)
    {
        SettingsDSP::find($request->get('id'))->update([
            'spendlimit' => (int)$request->get('spendlimit', 0),
        ]);
        return response('success', 200);
    }

    public function changeCommentVue(Request $request)
    {
        SettingsDSP::find($request->get('id'))->update([
            'comments' => $request->get('comments'),
        ]);
        return response('success', 200);
    }

    public function approveEndpointVue($id)
    {
        $dspSettings = SettingsDSP::find($id);
        if (!$dspSettings) {
            return response('no data', 404);
        }

        $dspSettings->active = SettingsDSP::STATUS_ACTIVE;
        if ($dspSettings->save()) {
            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
                (new ListDSP)->where('company_name', $dspSettings->company_name)->update(['cap_reached' => 0]);
            }
            return response('success');
        } else {
            return response('error', 404);
        }
    }

    public function archiveEndpointVue($id)
    {
        SettingsDSP::find($id)->update([
            'active' => SettingsDSP::STATUS_ARCHIVE,
            'deleted_at' => Carbon::now()->toDateTimeString(),
        ]);

        return response('success');
    }

    public function deleteEndpointVue($id)
    {
        if (!in_array(Auth::user()->role, User::ROLES_DELETE_ENDPOINT)) {
            return response('no permissions', 403);
        }

        $dspSettings = SettingsDSP::find($id);
        if (!$dspSettings) {
            return response('no data', 404);
        }

        if ((new Stats)->endpointHasImpressions($dspSettings->keyname, 'dsp')) {
            return $this->archiveEndpointVue($id);
        }

        if ($dspSettings->delete()) {
            return response('success');
        } else {
            return response('error', 404);
        }
    }

    public function restoreEndpointVue($id)
    {
        SettingsDSP::find($id)->update([
            'active' => SettingsDSP::STATUS_NOT_ACTIVE,
            'deleted_at' => null,
        ]);

        return response('success');
    }

    public function approve(Request $request)
    {
        if (!$request->ajax() || !$request->has('id')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $dspSettings = (new SettingsDSP)->find($id);

        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->active = 1;
        if ($dspSettings->save()) {

            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
                (new ListDSP)->where('company_name', $dspSettings->company_name)->update(['cap_reached' => 0]);
            }

            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function archive(Request $request)
    {
        if (!$request->ajax() || !$request->has('id')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $dspSettings = (new SettingsDSP)->find($id);

        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->active = SettingsDSP::STATUS_ARCHIVE;
        $dspSettings->deleted_at = Carbon::now()->toDateTimeString();
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function restore(Request $request)
    {
        if (!$request->ajax() || !$request->has('id')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $dspSettings = (new SettingsDSP)->find($id);

        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->active = SettingsDSP::STATUS_NOT_ACTIVE;
        $dspSettings->deleted_at = null;
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function delete(Request $request)
    {
        if (!$request->ajax() || !$request->has('id')) {
            return response('Not Found', 404);
        }

        if(!in_array(Auth::user()->role, User::ROLES_DELETE_ENDPOINT)){
            return 'No permissions';
        }

        $id = (int)$request->id;
        $dspSettings = (new SettingsDSP)->find($id);

        if (!$dspSettings) {
            return 'No data';
        }

        if((new Stats)->endpointHasImpressions($dspSettings->keyname, 'dsp')){
            return $this->archive($request);
        }

        if ($dspSettings->delete()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function saveEndpoint(Request $request)
    {
        if (!$request->ajax() || !$request->has('id') || !$request->has('endpoint')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $endpoint = clean($request->endpoint);

        $dspSettings = (new SettingsDSP)->find($id);
        if (!$dspSettings || !$endpoint) {
            return 'No data';
        }

        $dspSettings->endpoint = $dspSettings->useVast ? $endpoint : clearTrimHttp($endpoint);
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function saveQps(Request $request)
    {
        if (!$request->ajax() || !$request->has('id') || !$request->has('qps')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $qps = (int)$request->qps;

        $dspSettings = (new SettingsDSP)->find($id);
        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->qps = $qps;
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function saveLimit(Request $request)
    {
        if (!$request->ajax() || !$request->has('id') || !$request->has('spendLimit')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $spendLimit = (int)$request->spendLimit;

        $dspSettings = (new SettingsDSP)->find($id);
        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->spendlimit = $spendLimit;
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }


    public function addComment(Request $request)
    {
        if (!$request->ajax() || !$request->has('id') || !$request->has('dspComments')) {
            return response('Not Found', 404);
        }

        $id = (int)$request->id;
        $dspComments = trim($request->dspComments);

        $dspSettings = (new SettingsDSP)->find($id);
        if (!$dspSettings) {
            return 'No data';
        }

        $dspSettings->comments = $dspComments;
        if ($dspSettings->save()) {
            return 'ok';
        } else {
            return 'Sorry, error database';
        }
    }

    /**
     * @return \Illuminate\Contracts\View\Factory|\Illuminate\View\View
     */
    public function info()
    {
        $title = 'DSP Info';

        return view('vue', compact('title'));
    }

    public function nurlErrorsVue()
    {
        $title = 'Nurl call errors';

        return view('vue', compact('title'));
    }

    public function loadInfo(Request $request)
    {
        $filters = [];

        $dspName = $request->has('dsp_name') ? $request->dsp_name : null;
        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::INTEGRAL_STREAM])) {
            $lastHour = (new DspInfoStats)->getHourLastedStat();
        } else {
            $lastHour = (new DspHourlyStats)->getHourLastedStat();
        }
        $hourSearch = $request->has('hour') ? (int)$request->hour : $lastHour;
        $filters['searchType'] = $request->has('searchType') ? $request->searchType : 'company';

        if ($dspName) {
            $filters['dsp_name'] = $dspName;
        }
        if (!$dspName) {
            $filters['hour'] = $hourSearch;
        }

        $filters['page'] = $request->has('page') ? (int)$request->page : 1;
        $filters['per_page'] = 100;
        $filters['region'] = $request->has('region') ? $request->region : null;
        $filters['manager'] = $request->has('manager') ? $request->manager : null;
        $filters['orderBy'] = $request->has('orderBy') ? $request->orderBy : 'date';
        $filters['sortedBy'] = $request->has('sortedBy') ? $request->sortedBy : 'desc';
        $filters['export'] = $request->has('export') ? $request->export : false;

        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK, Project::ADVENUE, Project::ACEEX, Project::INTEGRAL_STREAM])) {
            $dspInfo = (new DspInfoStats)->getDspInfo($filters);
        } else {
            $dspInfo = (new DspHourlyStats)->getDspInfo($filters);
        }
        $hourSearch = str_pad($hourSearch, 2, "0", STR_PAD_LEFT);
        $date = ($hourSearch >= date('H')) ? (new DateTime)->modify('-1 day')->format('Y-m-d') : (new DateTime)->format('Y-m-d');
        $date = DateTime::createFromFormat("Y-m-d H:00:00", "$date $hourSearch:00:00");
        if (!$date) {
            return abort(404);
        }
        $dateHour = $date->format("Y-m-d H:00:00");

        $dspInfoBlockeds = null;
        $dspInfoExamples = null;
        $dspInfoStatusCodes = null;

        if (in_array(app()->project->name, [Project::ACUITY, Project::ACEEX])) {
            $dspIds = array_unique(array_column(is_array($dspInfo) ? $dspInfo : $dspInfo->items(), 'id'));
            if ($dspIds) {
                $dspInfoBlockeds = $this->getDspHourlyStatsBlockeds($dateHour, $dspIds);
                $dspInfoExamples = $this->getDspHourlyStatsExamples($dateHour, $dspIds);
                $dspInfoStatusCodes = $this->getDspHourlyStatsStatusCodes($dateHour, $dspIds);
            }

        }

        if ($request->export && $request->ext) {
            $this->download_report($dspInfo, $request->ext);
        }

        return response()->json([
            'lastHour' => $hourSearch,
            'data' => $dspInfo,
            'blockeds' => $dspInfoBlockeds,
            'examples' => $dspInfoExamples,
            'statusCodes' => $dspInfoStatusCodes,
        ]);
    }

    public function nurlErrorsLoadInfo(Request $request)
    {
        $params = [];
        $params['searchType'] = $request->has('searchType') ? $request->searchType : 'company';
        $params['dateFrom'] = $request->dateFrom;
        $params['dateTo'] = $request->dateTo;
        $params['manager'] = $request->has('manager') ? $request->manager : null;
        $params['region'] = $request->has('region') ? $request->region : null;
        $params['dsp_name'] = $request->has('dsp_name') ? $request->dsp_name : null;
        $orderBy = $request->has('orderBy') ? $request->orderBy : 'date';
        $sortedBy = $request->has('sortedBy') ? $request->sortedBy : 'desc';
        $currentPage = $request->has('page') ? (int)$request->page : 1;
        $rowsPerPage = 100;
        $export = $request->has('export') ? $request->export : false;

        $nurlErrors = (new NurlErrors)->getNurlErrorsFiltered($params, $orderBy, $sortedBy, $currentPage, $rowsPerPage);

        if ($export) {
            $nurlErrorsFull = $nurlErrors['data'];
            $ext = $request->has('ext') ? $request->ext : 'xlsx';
            $headers = [
                'Date',
                'DSP name',
                'Count',
                'Nurls'
            ];
            $columns = [
                'date',
                'keyname',
                'count',
                'nurls',
            ];

            while ($currentPage < $nurlErrors['last_page']) {
                $currentPage++;
                Paginator::currentPageResolver(function () use ($currentPage) {
                    return $currentPage;
                });
                $nurlErrors = (new NurlErrors)->getNurlErrorsFiltered($params, $orderBy, $sortedBy, $currentPage, $rowsPerPage);
                $nurlErrorsFull = array_merge($nurlErrorsFull, $nurlErrors['data']);
            }


            $this->exportToFile($headers, $columns, $nurlErrorsFull, $ext, 2);

            return response('success', 200);
        }

        return response()->json([
            'data' => $nurlErrors,
        ]);
    }

    private function getDspHourlyStatsExamples($dateHour, $dspIds)
    {
        $db = app(Client::class);

        $date = new DateTime("now", new DateTimeZone('UTC'));
        $oneDayAgoDate = $date->sub(new DateInterval('P1D'))->format('Y-m-d H:00:00');

        $whereDate = count($dspIds) == 1 ? "time >= '{$oneDayAgoDate}'" : "time = '{$dateHour}'";
        $dspIds = implode(',', $dspIds);

        if (session()->has('current_db') && session()->get('current_db') == 'gothamads') {
            $tblName = 'gothamDspHourlyStatsExamples';
        } elseif (session()->has('current_db') && session()->get('current_db') == 'bizzclick') {
            $tblName = 'bizzclickDspHourlyStatsExamples';
        } elseif (app()->project->name === Project::ACEEX2) {
            $tblName = 'dspHourlyStatsNaebExamples';
        } else {
            $tblName = 'dspHourlyStatsExamples';
        }

        $clickHouseArr = [];
        $sql = "SELECT dspId, time, errorName, groupArray(example) as examples
                FROM $tblName
                WHERE $whereDate
                  AND dspId IN ($dspIds)
                GROUP BY dspId, time, errorName
                ORDER BY dspId, time, errorName";

        $clickHouseStat = $db->select($sql);

        if ($clickHouseStat->count() > 0) {
            foreach ($clickHouseStat->rows() as $exapmleRow) {
                $clickHouseArr[$exapmleRow['dspId']][$exapmleRow['time']][$exapmleRow['errorName']] = $exapmleRow;
            }
        }

        return $clickHouseArr;
    }

    private function getDspHourlyStatsStatusCodes($dateHour, $dspIds)
    {
        $db = app(Client::class);

        $date = new DateTime("now", new DateTimeZone('UTC'));
        $oneDayAgoDate = $date->sub(new DateInterval('P1D'))->format('Y-m-d H:00:00');

        $whereDate = count($dspIds) == 1 ? "time >= '{$oneDayAgoDate}'" : "time = '{$dateHour}'";
        $dspIds = implode(',', $dspIds);

        if (session()->has('current_db') && session()->get('current_db') == 'gothamads') {
            $tblName = 'gothamDspHourlyStatsStatusCodes';
        } elseif (session()->has('current_db') && session()->get('current_db') == 'bizzclick') {
            $tblName = 'bizzclickDspHourlyStatsStatusCodes';
        } elseif (app()->project->name === Project::ACEEX2) {
            $tblName = 'dspHourlyStatsNaebStatusCodes';
        } else {
            $tblName = 'dspHourlyStatsStatusCodes';
        }

        $clickHouseArr = [];
        $sql = "SELECT dspId, time, groupUniqArray(errorName) as errorsName, groupArray(statusCodes) as statusCodes
                FROM $tblName
                WHERE $whereDate
                  AND dspId IN ($dspIds)
                GROUP BY dspId, time
                ORDER BY dspId, time";

        $clickHouseStat = $db->select($sql);

        if ($clickHouseStat->count() > 0) {
            foreach ($clickHouseStat->rows() as $exapmleRow) {
                $clickHouseArr[$exapmleRow['dspId']][$exapmleRow['time']] = implode('<br>', $exapmleRow['statusCodes']);
            }
        }

        return $clickHouseArr;
    }

    private function getDspHourlyStatsBlockeds($dateHour, $dspIds)
    {
        $db = app(Client::class);

        $date = new DateTime("now", new DateTimeZone('UTC'));
        $oneDayAgoDate = $date->sub(new DateInterval('P1D'))->format('Y-m-d H:00:00');

        $whereDate = count($dspIds) == 1 ? "time >= '{$oneDayAgoDate}'" : "time = '{$dateHour}'";
        $dspIds = implode(',', $dspIds);

        if (session()->has('current_db') && session()->get('current_db') == 'gothamads') {
            $tblName = 'gothamDspHourlyStatsBlockeds';
        } elseif (session()->has('current_db') && session()->get('current_db') == 'bizzclick') {
            $tblName = 'bizzclickDspHourlyStatsBlockeds';
        } elseif (app()->project->name === Project::ACEEX2) {
            $tblName = 'dspHourlyStatsNaebBlockeds';
        } else {
            $tblName = 'dspHourlyStatsBlockeds';
        }

        $clickHouseArr = [];
        $sql = "SELECT dspId, time, groupUniqArray(errorName) as errorsName, groupArray(blockeds) as blockeds
                FROM $tblName
                WHERE $whereDate
                  AND dspId IN ($dspIds)
                GROUP BY dspId, time
                ORDER BY dspId, time";

        $clickHouseStat = $db->select($sql);

        if ($clickHouseStat->count() > 0) {
            foreach ($clickHouseStat->rows() as $exapmleRow) {
                $blockedsDomainAndCrids = '';
                foreach ($exapmleRow['blockeds'] as $blockeds) {
                    $blockeds = preg_replace('/(\["|"\])/', '', $blockeds);
                    $tmpArr = explode('","', $blockeds);
                    $blockedsDomainAndCrids .= implode('<br>', $tmpArr) . "<br>";
                }
                $clickHouseArr[$exapmleRow['dspId']][$exapmleRow['time']] = $blockedsDomainAndCrids;
            }
        }
        return $clickHouseArr;
    }


    private function exportToFile($headers, $columns, $report, $ext, $floatPrecision)
    {

        $xls = new Spreadsheet();

        // Устанавливаем индекс активного листа
        $xls->setActiveSheetIndex(0);
        // Получаем активный лист
        $sheet = $xls->getActiveSheet();
        // Подписываем лист
        $sheet->setTitle('DSP List');
        $sheet->getColumnDimensionByColumn(2)->setWidth(30);

        if ($report) {
            $data[] = $headers;

            foreach ($report as $day) {
                $columnRes = array();
                foreach ($columns as $col) {
                    if (is_float($day["$col"])) {
                        $day["$col"] = round($day["$col"], $floatPrecision);
                    }
                    $columnRes[] = $day["$col"];
                };

                $data[] = $columnRes;
            }
        }

        // Print report
        if (!empty($data)) {
            for ($row = 0; $row < count($data); $row++) {
                $count_columns = count($data[$row]);
                for ($column = 0; $column < $count_columns; $column++) {
                    $sheet->setCellValueByColumnAndRow($column + 1, $row + 1, $data[$row][$column]);
                    if ($column <= 1) {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getAlignment()->setHorizontal('left');
                    } else {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getAlignment()->setHorizontal('right');
                    }
                    if ($row == 0) {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getFont()->setBold(true);
                    }
                }
            }
        }

        $ext = in_array($ext, ['xlsx', 'xls', 'ods', 'csv']) ? $ext : 'xls';

        // Выводим HTTP-заголовки
        header("Expires: 0");
        header("Cache-Control: no-cache, no-store, must-revalidate");
        header("Pragma: no-cache");
        header("Content-type: application/vnd.ms-excel");
        header("Content-Disposition: attachment; filename=list-dsp.$ext");
        // Выводим содержимое файла
        $objWriter = \PhpOffice\PhpSpreadsheet\IOFactory::createWriter($xls, ucfirst($ext));
        $objWriter->save('php://output');
        exit;
    }

    private function download_report($report, $ext)
    {

        $xls = new Spreadsheet();

        // Устанавливаем индекс активного листа
        $xls->setActiveSheetIndex(0);
        // Получаем активный лист
        $sheet = $xls->getActiveSheet();
        // Подписываем лист
        $sheet->setTitle('DSP Info');

        if ($report) {
            $data[] = [
                'Date',
                'DSP Name',
                'Region',
                'Bid Requests',
                'Bid Responses',
                'BPS',
                'BPS (%)',
                'Valid Bid Responses',
                'Valid To Total Responses (%)',
                'Timeouts',
                'Timeouts Percentage (%)'
            ];

            if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::ACEEX, Project::INTEGRAL_STREAM])) {
                $report = $report['data'];
            }
            foreach ($report as $day) {
                $data[] = [
                    $day['date'],
                    $day['keyname'],
                    $day['region'],
                    $day['bid_requests'],
                    $day['bid_responses'],
                    $day['bps'],
                    $day['bid_responses_percentage'],
                    (int)$day['valid_bid_responses'],
                    (float)$day['valid_responses_percentage'],
                    $day['timeouts'],
                    $day['timeouts_percentage'],
                ];
            }
        }

        // Print report
        if (!empty($data)) {
            for ($row = 0; $row < count($data); $row++) {
                $count_columns = count($data[$row]);
                for ($column = 0; $column < $count_columns; $column++) {
                    $sheet->setCellValueByColumnAndRow($column + 1, $row + 1, $data[$row][$column]);
                    if ($column == 0) {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getAlignment()->setHorizontal('left');
                    } else {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getAlignment()->setHorizontal('right');
                    }
                    if ($row == 0 || $data[$row][0] == 'Total' || $data[$row][$count_columns - 1] === 'N/A') {
                        $sheet->getStyleByColumnAndRow($column + 1, $row + 1)->getFont()->setBold(true);
                    }
                }
            }
        }

        $ext = in_array($ext, ['xlsx', 'xls', 'ods', 'csv']) ? $ext : 'xls';

        // Выводим HTTP-заголовки
        header("Expires: 0");
        header("Cache-Control: no-cache, no-store, must-revalidate");
        header("Pragma: no-cache");
        header("Content-type: application/vnd.ms-excel");
        header("Content-Disposition: attachment; filename=dsp-info.$ext");
        // Выводим содержимое файла
        $objWriter = \PhpOffice\PhpSpreadsheet\IOFactory::createWriter($xls, ucfirst($ext));
        $objWriter->save('php://output');
        exit;
    }

    public function managersVue()
    {
        $title = 'DSP Managers';

        return view('vue', compact('title'));
    }

    public function managers(Request $request)
    {

        $listDSP = (new ListDSP)->getListCompany();
        $listAllManagers = (new User)->getListManagers();
        $listManagers = [];
        $roles = in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK]) ?
            ['smanager', 'manager'] : User::ROLES_MANAGERS;
        foreach ($listAllManagers as $id => $array){
            if (in_array($array["role"], $roles)) {
                $listManagers[$id] = $array;
            }
        }
        return response()->json([
            'list_dsp_company' => $listDSP,
            'list_managers' => $listManagers,
        ]);

    }

    public function changeDspManager(Request $request)
    {


        if (!empty($request->manager_type) && isset($request->manager_id) && isset($request->company_name)) {
            $result = (new ListDSP)->changeDspManager($request->manager_type, $request->manager_id, $request->company_name);

            if ($result) {
                echo "manager changed";
            } else {
                echo 'Sorry. error change';
            }
        }
        return;
    }

    public function partnerStatPreferred(Request $request)
    {
        if (!empty($_POST['company'])) {
            $company = clean($_POST['company']);
            $partnerStatPreferred = (int)$_POST['partnerStatPreferred'];
            $result = (new ListDSP)->updatePartnerStatPreferred($company, $partnerStatPreferred);
            if ($result) {
                echo 'ok';
            } else {
                echo 'Sorry error change';
            }
        }
    }

    public function setCompanyComment(Request $request)
    {
        if (!$request->ajax() || !$request->has('name') || !$request->has('comment')) {
            return response('Not Found', 404);
        }

        $name = $request->name;
        //$comment = clean($request->comment) ?: NULL;
        $comment = $request->comment ?: NULL;

        $company = (new ListDSP)->where('company_name', $name)->first();
        if (!$company) {
            return 'No data';
        }

        $company->comment = $comment;

        if ($company->save()) {
            echo "ok";
        } else {
            echo 'Sorry, error database';
        }
        return;
    }

    public function getCompanyComment(Request $request)
    {
        if (!$request->ajax() || !$request->has('name')) {
            return response('Not Found', 404);
        }

        $name = $request->name;

        $company = (new ListDSP)->where('company_name', $name)->first();

        if (!$company) {
            return 'No data';
        }

        die($company->comment);

    }

    public function companyTypeUpdate(Request $request)
    {
        if (!in_array(app()->project->name, [Project::SMARTY_ADS, Project::GOTHAM_ADS, Project::BIZZ_CLICK])) {
            return abort(404);
        }

        if (!$request->has('scadNetworkId') || !$request->has('id')) {
            return abort(404);
        }

        $company = (new ListDSP)->find($request->id);

        if (!$company) {
            return abort(404);
        }

        $skadNetworkArray = explode(' ', $request->scadNetworkId);
        sort($skadNetworkArray);
        $skadNetworkArray = array_unique($skadNetworkArray);
        $company->scadNetworkId = implode(' ', $skadNetworkArray);
        $company->save();

        return response($company, 200);
    }
}
